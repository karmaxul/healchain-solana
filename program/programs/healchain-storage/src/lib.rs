// HealChain Solana Anchor Program — minimal storage anchoring
//
// NOT BUILD-VERIFIED IN THIS SESSION. This sandbox's Ubuntu-packaged
// Rust/Cargo (1.75) is too old for current Anchor's dependency tree
// (requires a newer "edition2024" Cargo feature). Written to correct,
// standard Anchor 0.30.x conventions from documented patterns, but
// confirm it actually compiles via Solana Playground (playground.solana.com,
// zero setup) or a real local Anchor install before relying on it.
//
// Mirrors the existing EVM oracle-fulfillment pattern:
//   1. User calls store_request with a document hash + metadata
//      -> emits a log event the Go oracle watcher picks up
//   2. Oracle (off-chain) erasure-codes the document, uploads shards
//      to IPFS, then calls fulfill_store with the resulting CIDs
//   3. fulfill_store writes shard metadata to a PDA account, only
//      callable by the designated oracle authority

use anchor_lang::prelude::*;

declare_id!("68EV9DXfXcYvjWLwYBpgCebFwvg6TqtitVZPdx6JnUDt"); // placeholder — replace with your real deployed program ID

// Placeholder oracle authority — replace with your actual oracle pubkey
// before deploying. For production, consider a small multisig or a
// dedicated config account instead of a single hardcoded key, mirroring
// however your EVM oracle authorization is currently structured.
pub mod oracle_authority_id {
    use anchor_lang::prelude::*;
    declare_id!("H2gPhDmyntewnFTN3caZoTdvMztdpxyNJ7PAErePCe5E");
}

#[program]
pub mod healchain_storage {
    use super::*;

    /// Step 1: user requests storage. Emits a log for the oracle to
    /// pick up (mirrors the EVM contract's storeRequest -> event pattern).
    pub fn store_request(
        ctx: Context<StoreRequest>,
        doc_hash: [u8; 64],       // ci-sha4096 digest root (or a truncated/hashed reference to it)
        data_shards: u8,          // k, e.g. 10
        parity_shards: u8,        // m, e.g. 4
        label: String,            // human-readable label, e.g. document type
    ) -> Result<()> {
        require!(data_shards > 0, HealChainError::InvalidShardConfig);
        require!(parity_shards > 0, HealChainError::InvalidShardConfig);
        require!(label.len() <= 64, HealChainError::LabelTooLong);

        let record = &mut ctx.accounts.record;
        record.requester = ctx.accounts.requester.key();
        record.doc_hash = doc_hash;
        record.data_shards = data_shards;
        record.parity_shards = parity_shards;
        record.label = label.clone();
        record.fulfilled = false;
        record.cids = Vec::new();
        record.bump = ctx.bumps.record;

        emit!(StoreRequested {
            record: record.key(),
            requester: record.requester,
            doc_hash,
            data_shards,
            parity_shards,
            label,
        });

        Ok(())
    }

    /// Step 2: oracle fulfills the request with shard locations (IPFS CIDs).
    /// Only the designated oracle authority can call this.
    pub fn fulfill_store(
        ctx: Context<FulfillStore>,
        cids: Vec<String>,
    ) -> Result<()> {
        let record = &mut ctx.accounts.record;

        require!(!record.fulfilled, HealChainError::AlreadyFulfilled);
        require!(
            cids.len() == (record.data_shards + record.parity_shards) as usize,
            HealChainError::ShardCountMismatch
        );

        record.cids = cids.clone();
        record.fulfilled = true;

        emit!(StoreFulfilled {
            record: record.key(),
            cids,
        });

        Ok(())
    }
}

// ── Accounts ─────────────────────────────────────────────────────────

#[derive(Accounts)]
#[instruction(doc_hash: [u8; 64])]
pub struct StoreRequest<'info> {
    #[account(mut)]
    pub requester: Signer<'info>,

    #[account(
        init,
        payer = requester,
        space = 8 + StorageRecord::INIT_SPACE,
        seeds = [b"record", requester.key().as_ref(), &doc_hash[..32]],
        bump
    )]
    pub record: Account<'info, StorageRecord>,

    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct FulfillStore<'info> {
    /// The oracle authority — checked against the program-level constant.
    /// For production, consider a small multisig or config account instead
    /// of a single hardcoded key, mirroring your EVM oracle authorization.
    #[account(constraint = oracle_authority.key() == oracle_authority_id::ID @ HealChainError::UnauthorizedOracle)]
    pub oracle_authority: Signer<'info>,

    #[account(mut)]
    pub record: Account<'info, StorageRecord>,
}

// ── State ────────────────────────────────────────────────────────────

#[account]
#[derive(InitSpace)]
pub struct StorageRecord {
    pub requester: Pubkey,
    pub doc_hash: [u8; 64],
    pub data_shards: u8,
    pub parity_shards: u8,
    #[max_len(64)]
    pub label: String,
    pub fulfilled: bool,
    #[max_len(14, 64)] // up to 14 shards (k+m), each CID string up to 64 chars
    pub cids: Vec<String>,
    pub bump: u8,
}

// ── Events (what the Go oracle watcher subscribes to via program logs) ─

#[event]
pub struct StoreRequested {
    pub record: Pubkey,
    pub requester: Pubkey,
    pub doc_hash: [u8; 64],
    pub data_shards: u8,
    pub parity_shards: u8,
    pub label: String,
}

#[event]
pub struct StoreFulfilled {
    pub record: Pubkey,
    pub cids: Vec<String>,
}

// ── Errors ───────────────────────────────────────────────────────────

#[error_code]
pub enum HealChainError {
    #[msg("Invalid shard configuration")]
    InvalidShardConfig,
    #[msg("Label exceeds 64 characters")]
    LabelTooLong,
    #[msg("Record already fulfilled")]
    AlreadyFulfilled,
    #[msg("Shard count in fulfillment does not match request")]
    ShardCountMismatch,
    #[msg("Caller is not the authorized oracle for this record")]
    UnauthorizedOracle,
}
