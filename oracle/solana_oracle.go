// solana_oracle.go — HealChain Solana oracle watcher
//
// NOT BUILD-VERIFIED (no Go toolchain available in the sandbox that wrote
// this). Written from documented gagliardetto/solana-go patterns and
// mirrors the existing EVM oracle-watcher structure this project already
// runs in production. Confirm it compiles with `go build` locally before
// relying on it, same caveat as the Anchor program.
//
// Mirrors the EVM oracle pattern (per repair_oracle.go / oracle.go):
//   EVM:    watch eth_getLogs for a StorageRequested event -> fetch/verify
//           -> submit fulfillment tx
//   Solana: subscribe to program logs via WebSocket -> parse the
//           StoreRequested event from the log -> fetch/verify -> call
//           fulfill_store
//
// Install the dependency first:
//   go get github.com/gagliardetto/solana-go
//   go get github.com/gagliardetto/solana-go/rpc
//   go get github.com/gagliardetto/solana-go/rpc/ws

package solanaoracle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/ws"
)

// Replace with your actual deployed program ID (the same one in declare_id!
// in lib.rs — 68EV9DXfXcYvjWLwYBpgCebFwvg6TqtitVZPdx6JnUDt as of this session).
var HealChainProgramID = solana.MustPublicKeyFromBase58(
	"68EV9DXfXcYvjWLwYBpgCebFwvg6TqtitVZPdx6JnUDt",
)

// StoreRequestedEvent mirrors the on-chain StoreRequested event struct.
// Anchor events are Borsh-serialized and logged base64-encoded, prefixed
// with an 8-byte discriminator (first 8 bytes of sha256("event:StoreRequested")).
// Parsing this properly requires either:
//   (a) using the Anchor-generated IDL + an IDL-aware decoder, or
//   (b) manually matching the discriminator and decoding the Borsh fields
//       in the same order they're declared in the Rust struct.
// This scaffold shows the manual approach (b) since it doesn't require
// pulling in additional IDL-parsing tooling -- but (a) is more robust
// long-term, worth revisiting once this is working end-to-end.
type StoreRequestedEvent struct {
	Record       solana.PublicKey
	Requester    solana.PublicKey
	DocHash      [64]byte
	DataShards   uint8
	ParityShards uint8
	Label        string
}

// backendClient talks to the private HealChain backend over plain HTTP --
// the only connection point to anything private. No Go imports of
// private packages anywhere in this file.
type backendClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func newBackendClient(baseURL, apiKey string) *backendClient {
	return &backendClient{baseURL: baseURL, apiKey: apiKey, http: &http.Client{}}
}

type processAndStoreRequest struct {
	DocHash      string `json:"doc_hash"`
	DataShards   int    `json:"data_shards"`
	ParityShards int    `json:"parity_shards"`
}
type processAndStoreResponse struct {
	CIDs []string `json:"cids"`
}

// ProcessAndStore calls the private backend's single boundary endpoint --
// it hands over the document's ci-sha4096 hash (hex-encoded) and shard
// config, and gets back finished IPFS CIDs. Raw document bytes, IPFS
// credentials, and all erasure-coding logic stay entirely on the
// private side.
func (b *backendClient) ProcessAndStore(ctx context.Context, docHashHex string, dataShards, parityShards int) ([]string, error) {
	reqBody, err := json.Marshal(processAndStoreRequest{
		DocHash: docHashHex, DataShards: dataShards, ParityShards: parityShards,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", b.baseURL+"/internal/solana/process-and-store", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-Key", b.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling backend: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("backend returned status %d", resp.StatusCode)
	}
	var out processAndStoreResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.CIDs, nil
}

// OracleWatcher holds the WebSocket connection and RPC client for
// watching HealChain's Solana program.
type OracleWatcher struct {
	rpcClient       *rpc.Client
	wsClient        *ws.Client
	programID       solana.PublicKey
	oracleAuthority solana.PrivateKey
	backend         *backendClient
}

func NewOracleWatcher(ctx context.Context, rpcURL, wsURL, oracleKeypairPath, backendURL, backendAPIKey string) (*OracleWatcher, error) {
	rpcClient := rpc.New(rpcURL)

	wsClient, err := ws.Connect(ctx, wsURL)
	if err != nil {
		return nil, fmt.Errorf("solana ws connect failed: %w", err)
	}

	oracleKey, err := solana.PrivateKeyFromSolanaKeygenFile(oracleKeypairPath)
	if err != nil {
		return nil, fmt.Errorf("loading oracle authority keypair: %w", err)
	}

	return &OracleWatcher{
		rpcClient:       rpcClient,
		wsClient:        wsClient,
		programID:       HealChainProgramID,
		oracleAuthority: oracleKey,
		backend:         newBackendClient(backendURL, backendAPIKey),
	}, nil
}

// Watch subscribes to program logs and processes StoreRequested events as
// they arrive. This is the Solana equivalent of the EVM oracle's
// eth_getLogs polling loop (per oracle.go), but push-based via WebSocket
// rather than poll-based.
func (w *OracleWatcher) Watch(ctx context.Context) error {
	sub, err := w.wsClient.LogsSubscribeMentions(w.programID, rpc.CommitmentConfirmed)
	if err != nil {
		return fmt.Errorf("logs subscribe failed: %w", err)
	}
	defer sub.Unsubscribe()

	log.Printf("[solana-oracle] watching program logs for %s", w.programID.String())

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		got, err := sub.Recv(ctx)
		if err != nil {
			log.Printf("[solana-oracle] subscription recv error: %v", err)
			continue
		}
		if got.Value.Err != nil {
			// Transaction that mentioned the program failed -- not our concern.
			continue
		}

		event, ok := parseStoreRequestedFromLogs(got.Value.Logs)
		if !ok {
			continue // no matching event in this transaction's logs
		}

		log.Printf("[solana-oracle] StoreRequested: record=%s requester=%s label=%q shards=%d+%d",
			event.Record, event.Requester, event.Label, event.DataShards, event.ParityShards)

		if err := w.handleStoreRequested(ctx, event); err != nil {
			log.Printf("[solana-oracle] ERROR handling StoreRequested for record=%s: %v",
				event.Record, err)
			// Consider retry/dead-letter handling here, mirroring whatever
			// the EVM oracle does on fulfillment failure (see repair_oracle.go).
		}
	}
}

// parseStoreRequestedFromLogs scans a transaction's log lines for an Anchor
// event log ("Program data: <base64>"), and attempts to decode it as a
// StoreRequestedEvent. Returns ok=false if no matching event is found.
//
// NOT VERIFIED against real Anchor 1.1.2 log output format -- confirm the
// exact log prefix and discriminator bytes empirically once you have a
// real devnet transaction to inspect (call store_request, then run
// `solana logs <program_id>` to see the actual format before trusting
// this parsing logic).
// storeRequestedDiscriminator is the first 8 bytes of sha256("event:StoreRequested"),
// Anchor's standard event-discriminator scheme. Computed once at package init.
var storeRequestedDiscriminator [8]byte

func init() {
	hash := sha256.Sum256([]byte("event:StoreRequested"))
	copy(storeRequestedDiscriminator[:], hash[:8])
}

func parseStoreRequestedFromLogs(logs []string) (StoreRequestedEvent, bool) {
	const dataPrefix = "Program data: "
	for _, l := range logs {
		if !strings.HasPrefix(l, dataPrefix) {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(l, dataPrefix))
		if err != nil {
			continue
		}
		if len(raw) < 8 {
			continue
		}
		if !bytes.Equal(raw[:8], storeRequestedDiscriminator[:]) {
			continue // not a StoreRequested event -- could be StoreFulfilled or something else
		}
		event, ok := decodeStoreRequestedBorsh(raw[8:])
		if ok {
			return event, true
		}
	}
	return StoreRequestedEvent{}, false
}

// decodeStoreRequestedBorsh manually decodes the Borsh-serialized event
// body (after the 8-byte discriminator). Field order MUST exactly match
// the Rust struct declaration order in lib.rs:
//   record, requester, doc_hash, data_shards, parity_shards, label
func decodeStoreRequestedBorsh(data []byte) (StoreRequestedEvent, bool) {
	var e StoreRequestedEvent
	offset := 0

	readPubkey := func() (solana.PublicKey, bool) {
		if offset+32 > len(data) {
			return solana.PublicKey{}, false
		}
		var pk solana.PublicKey
		copy(pk[:], data[offset:offset+32])
		offset += 32
		return pk, true
	}

	record, ok := readPubkey()
	if !ok {
		return e, false
	}
	requester, ok := readPubkey()
	if !ok {
		return e, false
	}

	if offset+64 > len(data) {
		return e, false
	}
	var docHash [64]byte
	copy(docHash[:], data[offset:offset+64])
	offset += 64

	if offset+2 > len(data) {
		return e, false
	}
	dataShards := data[offset]
	parityShards := data[offset+1]
	offset += 2

	if offset+4 > len(data) {
		return e, false
	}
	labelLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	if offset+int(labelLen) > len(data) {
		return e, false
	}
	label := string(data[offset : offset+int(labelLen)])

	e = StoreRequestedEvent{
		Record:       record,
		Requester:    requester,
		DocHash:      docHash,
		DataShards:   dataShards,
		ParityShards: parityShards,
		Label:        label,
	}
	return e, true
}

// handleStoreRequested calls the private backend's single boundary
// endpoint, which handles document lookup, erasure-coding, and IPFS
// upload entirely on the private side -- this file never sees raw
// document bytes, IPFS credentials, or any private package. This is
// the actual fix for the internal/ package import problem hit earlier:
// no private imports anywhere in this repo now, by design.
//
// The one open item is unchanged from before, just moved: the private
// backend's fetchPendingDocument (in solana_api.go, the private repo)
// still needs a real decision on how a client's document gets
// associated with a record PDA -- not something this file can solve.
func (w *OracleWatcher) handleStoreRequested(ctx context.Context, event StoreRequestedEvent) error {
	docHashHex := hex.EncodeToString(event.DocHash[:])
	cids, err := w.backend.ProcessAndStore(ctx, docHashHex, int(event.DataShards), int(event.ParityShards))
	if err != nil {
		return fmt.Errorf("ProcessAndStore: %w", err)
	}
	log.Printf("[solana-oracle] backend processed %d shards for record=%s", len(cids), event.Record)

	sig, err := w.submitFulfillStore(ctx, event.Record, cids)
	if err != nil {
		return fmt.Errorf("submitFulfillStore: %w", err)
	}
	log.Printf("[solana-oracle] fulfill_store confirmed for record=%s, sig=%s", event.Record, sig)

	return nil
}

// submitFulfillStore builds, signs, and sends the fulfill_store instruction,
// writing the shard CIDs back on-chain. Signed by the oracle authority
// keypair (loaded separately -- keep this file's oracle key path
// configurable, not hardcoded, once this moves toward production).
func (w *OracleWatcher) submitFulfillStore(ctx context.Context, record solana.PublicKey, cids []string) (string, error) {
	log.Println("[BUILD-MARKER] submitFulfillStore running build: fulfill-debug-v2")

	// DIAGNOSTIC 1: check what this RPC client actually sees for this account,
	// right before attempting to use it in a transaction.
	acctInfo, acctErr := w.rpcClient.GetAccountInfo(ctx, record)
	if acctErr != nil {
		log.Printf("[DIAGNOSTIC] GetAccountInfo error for record=%s: %v", record, acctErr)
	} else if acctInfo == nil || acctInfo.Value == nil {
		log.Printf("[DIAGNOSTIC] record=%s: account info is nil (RPC sees it as non-existent)", record)
	} else {
		log.Printf("[DIAGNOSTIC] record=%s: EXISTS -- owner=%s, lamports=%d, data_len=%d",
			record, acctInfo.Value.Owner, acctInfo.Value.Lamports, len(acctInfo.Value.Data.GetBinary()))
	}

	// Anchor instruction discriminator: first 8 bytes of sha256("global:fulfill_store")
	discHash := sha256.Sum256([]byte("global:fulfill_store"))
	var data []byte
	data = append(data, discHash[:8]...)

	// Borsh-encode Vec<String>: u32 length prefix, then each string as
	// (u32 length prefix + bytes)
	cidsLenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(cidsLenBuf, uint32(len(cids)))
	data = append(data, cidsLenBuf...)
	for _, cid := range cids {
		strLenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(strLenBuf, uint32(len(cid)))
		data = append(data, strLenBuf...)
		data = append(data, []byte(cid)...)
	}

	// DIAGNOSTIC 2: print the exact account metas before building the
	// instruction -- ground truth on addresses and flags, not assumption.
	log.Printf("[DIAGNOSTIC] account meta 0: pubkey=%s isSigner=true isWritable=false (oracle_authority)", w.oracleAuthority.PublicKey())
	log.Printf("[DIAGNOSTIC] account meta 1: pubkey=%s isSigner=false isWritable=true (record)", record)
	log.Printf("[DIAGNOSTIC] instruction data (hex): %x", data)
	log.Printf("[DIAGNOSTIC] instruction data length: %d bytes", len(data))

	instruction := solana.NewInstruction(
		w.programID,
		solana.AccountMetaSlice{
			solana.NewAccountMeta(w.oracleAuthority.PublicKey(), false, true), // oracle_authority: not writable, IS signer
			solana.NewAccountMeta(record, true, false),                       // record: writable, not signer
		},
		data,
	)

	recent, err := w.rpcClient.GetLatestBlockhash(ctx, rpc.CommitmentConfirmed)
	if err != nil {
		return "", fmt.Errorf("get blockhash: %w", err)
	}

	tx, err := solana.NewTransaction(
		[]solana.Instruction{instruction},
		recent.Value.Blockhash,
		solana.TransactionPayer(w.oracleAuthority.PublicKey()),
	)
	if err != nil {
		return "", fmt.Errorf("build tx: %w", err)
	}

	// DIAGNOSTIC 3: the single most direct check available -- what account
	// keys, in what order, actually ended up in the COMPILED message, as
	// opposed to what we assume solana-go did with the metas we gave it.
	log.Printf("[DIAGNOSTIC] compiled transaction account keys: %v", tx.Message.AccountKeys)

	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(w.oracleAuthority.PublicKey()) {
			return &w.oracleAuthority
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("sign tx: %w", err)
	}

	sig, err := w.rpcClient.SendTransactionWithOpts(ctx, tx, rpc.TransactionOpts{
		PreflightCommitment: rpc.CommitmentConfirmed,
	})
	if err != nil {
		return "", fmt.Errorf("send tx: %w", err)
	}

	return sig.String(), nil
}

