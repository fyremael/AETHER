package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveBearerTokenRejectsMutuallyExclusiveFlags(t *testing.T) {
	t.Helper()

	_, err := resolveBearerToken(
		"inline-token",
		"token.txt",
		func(string) string { return "" },
		func(string) ([]byte, error) { return nil, nil },
	)
	if err == nil {
		t.Fatalf("expected mutual exclusion error")
	}
}

func TestResolveBearerTokenPrefersTokenFile(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "pilot.token")
	if err := os.WriteFile(tokenPath, []byte("  token-from-file \n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	token, err := resolveBearerToken(
		"",
		tokenPath,
		func(string) string { return "" },
		os.ReadFile,
	)
	if err != nil {
		t.Fatalf("resolve token: %v", err)
	}
	if token != "token-from-file" {
		t.Fatalf("unexpected token %q", token)
	}
}

func TestResolveBearerTokenFallsBackToEnvironment(t *testing.T) {
	t.Helper()

	token, err := resolveBearerToken(
		"",
		"",
		func(name string) string {
			if name == "AETHER_TOKEN" {
				return "env-token"
			}
			return ""
		},
		func(string) ([]byte, error) { return nil, nil },
	)
	if err != nil {
		t.Fatalf("resolve env token: %v", err)
	}
	if token != "env-token" {
		t.Fatalf("unexpected token %q", token)
	}
}
