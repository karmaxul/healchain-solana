use {
    anchor_lang::{
        prelude::Pubkey,
        solana_program::{instruction::Instruction, system_program},
        AccountDeserialize, InstructionData, ToAccountMetas,
    },
    litesvm::LiteSVM,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
};

// Tests store_request only. fulfill_store is intentionally NOT tested here --
// it requires signing with the real oracle_authority keypair, whose private
// key lives in Bitwarden and should never be pasted into a test file. A
// separate test could use a throwaway test-only oracle pubkey if you want
// fulfill_store coverage later, but that requires temporarily pointing
// oracle_authority_id at a test key, not the real deployed one.

#[test]
fn test_store_request() {
    let program_id = healchain_storage::id();
    let payer = Keypair::new();

    let doc_hash: [u8; 64] = [7u8; 64]; // dummy 64-byte hash for the test
    let record = Pubkey::find_program_address(
        &[b"record", payer.pubkey().as_ref(), &doc_hash[..32]],
        &program_id,
    )
    .0;

    let mut svm = LiteSVM::new();
    let bytes = include_bytes!(concat!(
        env!("CARGO_TARGET_TMPDIR"),
        "/../deploy/healchain_storage.so"
    ));
    svm.add_program(program_id, bytes).unwrap();
    svm.airdrop(&payer.pubkey(), 1_000_000_000).unwrap(); // local simulator airdrop, not real/devnet SOL

    let data_shards: u8 = 10;
    let parity_shards: u8 = 4;
    let label = "test-document".to_string();

    let instruction = Instruction::new_with_bytes(
        program_id,
        &healchain_storage::instruction::StoreRequest {
            doc_hash,
            data_shards,
            parity_shards,
            label: label.clone(),
        }
        .data(),
        healchain_storage::accounts::StoreRequest {
            requester: payer.pubkey(),
            record,
            system_program: system_program::ID,
        }
        .to_account_metas(None),
    );

    let blockhash = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[instruction], Some(&payer.pubkey()), &blockhash);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[&payer]).unwrap();
    let res = svm.send_transaction(tx);
    assert!(res.is_ok(), "store_request transaction failed: {:?}", res);

    let record_account = svm.get_account(&record).unwrap();
    let mut data: &[u8] = &record_account.data;
    let record_state = healchain_storage::StorageRecord::try_deserialize(&mut data).unwrap();

    assert_eq!(record_state.requester, payer.pubkey());
    assert_eq!(record_state.doc_hash, doc_hash);
    assert_eq!(record_state.data_shards, data_shards);
    assert_eq!(record_state.parity_shards, parity_shards);
    assert_eq!(record_state.label, label);
    assert_eq!(record_state.fulfilled, false);
    assert!(record_state.cids.is_empty());
}
