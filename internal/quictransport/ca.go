package quictransport

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

// ClientTLSFromCA builds a pinning client config: caFile must be a PEM
// holding the certificate(s) to trust (for example the peer's persisted
// self-signed certificate produced by LoadOrCreateCert). This replaces
// the dev-grade InsecureSkipVerify path when federation peers pin each
// other's certificates.
func ClientTLSFromCA(caFile, serverName string) (*tls.Config, error) {
	pemBytes, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("afep/quic: read ca: %w", err)
	}
	pool := x509.NewCertPool()
	added := 0
	for rest := pemBytes; ; {
		var blk *pem.Block
		blk, rest = pem.Decode(rest)
		if blk == nil {
			break
		}
		if blk.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			return nil, fmt.Errorf("afep/quic: parse ca cert: %w", err)
		}
		pool.AddCert(cert)
		added++
	}
	if added == 0 {
		return nil, fmt.Errorf("afep/quic: no certificates in %s", caFile)
	}
	return &tls.Config{
		RootCAs:    pool,
		ServerName: serverName,
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{ALPN},
	}, nil
}
