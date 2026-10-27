package quictransport

import (
	"os"
	"path/filepath"
	"testing"
)

func TestClientTLSFromCAPinsPersistedCert(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "peer.crt")
	keyPath := filepath.Join(dir, "peer.key")
	if _, err := LoadOrCreateCert(certPath, keyPath); err != nil {
		t.Fatalf("LoadOrCreateCert: %v", err)
	}
	conf, err := ClientTLSFromCA(certPath, "peer.example")
	if err != nil {
		t.Fatalf("ClientTLSFromCA: %v", err)
	}
	if conf.RootCAs == nil {
		t.Fatal("RootCAs not set")
	}
	if conf.ServerName != "peer.example" {
		t.Fatalf("ServerName = %q", conf.ServerName)
	}
	if conf.InsecureSkipVerify {
		t.Fatal("pinning config must not skip verification")
	}
	if conf.MinVersion != 0x0304 { // tls.VersionTLS13
		t.Fatalf("MinVersion = %x", conf.MinVersion)
	}
}

func TestClientTLSFromCARejectsGarbage(t *testing.T) {
	dir := t.TempDir()
	garbage := filepath.Join(dir, "garbage.pem")
	if err := os.WriteFile(garbage, []byte("not a pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ClientTLSFromCA(garbage, "x"); err == nil {
		t.Fatal("want error for PEM without certificates")
	}
	if _, err := ClientTLSFromCA(filepath.Join(dir, "missing.pem"), "x"); err == nil {
		t.Fatal("want error for missing file")
	}
}
