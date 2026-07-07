// cmd/solanaoracle/main.go — runnable entrypoint for the public
// healchain-solana oracle. Lives in the PUBLIC repo now -- no private
// imports anywhere, connects to the private backend over plain HTTP.
//
// NOT BUILD-VERIFIED, same caveat as everything else this session.
//
// Requires two environment variables:
//   HEALCHAIN_BACKEND_URL     -- base URL of the private backend
//                                (e.g. https://api.healchain.org, or
//                                http://localhost:8080 for local testing)
//   SOLANA_ORACLE_API_KEY     -- shared secret matching the private
//                                backend's SOLANA_ORACLE_API_KEY env var
//                                (NOT a customer-facing API key -- generate
//                                a fresh, dedicated secret for this)
//
// Run: go run ./cmd/solanaoracle

package main

import (
	"context"
	"log"
	"os"

	"github.com/karmaxul/healchain-solana/oracle"

	"github.com/gagliardetto/solana-go/rpc"
)

func main() {
	ctx := context.Background()

	backendURL := os.Getenv("HEALCHAIN_BACKEND_URL")
	if backendURL == "" {
		log.Fatal("HEALCHAIN_BACKEND_URL environment variable is not set")
	}
	apiKey := os.Getenv("SOLANA_ORACLE_API_KEY")
	if apiKey == "" {
		log.Fatal("SOLANA_ORACLE_API_KEY environment variable is not set")
	}

	oracleKeyPath := os.Getenv("HOME") + "/ci-sha-project/healchain-storage/oracle-authority.json"

	watcher, err := solanaoracle.NewOracleWatcher(ctx, rpc.DevNet_RPC, rpc.DevNet_WS, oracleKeyPath, backendURL, apiKey)
	if err != nil {
		log.Fatalf("failed to start oracle watcher: %v", err)
	}

	log.Println("[solanaoracle] starting, connected to backend at", backendURL)

	if err := watcher.Watch(ctx); err != nil {
		log.Fatalf("watcher stopped: %v", err)
	}
}
