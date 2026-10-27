// Package transportquic provides a gRPC-over-QUIC transport: HTTP/2 framing
// (what gRPC speaks) carried inside QUIC streams — NAT-friendly, no
// head-of-line blocking across task threads, connection migration.
//
// NOTE: this is gRPC over QUIC streams with ALPN "afep-quic/0.1", not
// standardized HTTP/3; grpc-go has no production HTTP/3 transport yet.
package quictransport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"
)

// ALPN is the application protocol negotiated on every AFEP QUIC link.
const ALPN = "afep-quic/0.1"

// SelfSigned generates an ephemeral development certificate (localhost and
// loopback SANs, valid one year). Production deployments should pin a CA
// instead of skipping verification on the client side.
func SelfSigned() (tls.Certificate, error) {
	der, key, err := selfSignedMaterial()
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

// selfSignedMaterial builds the dev certificate DER + key (shared by
// SelfSigned and the persistent loader).
func selfSignedMaterial() ([]byte, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("afep/quic: keygen: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("afep/quic: serial: %w", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "afep-dev"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("afep/quic: cert: %w", err)
	}
	return der, key, nil
}

// LoadOrCreateCert persists the QUIC server certificate next to the
// identity seed: load the pair when present (stable federation identity
// across restarts — ephemeral certs changed the fingerprint on every
// boot), otherwise generate and write it (key 0600).
func LoadOrCreateCert(certFile, keyFile string) (tls.Certificate, error) {
	certPEM, errC := os.ReadFile(certFile)
	keyPEM, errK := os.ReadFile(keyFile)
	if errC == nil && errK == nil {
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("afep/quic: load cert pair: %w", err)
		}
		return cert, nil
	}
	der, key, err := selfSignedMaterial()
	if err != nil {
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("afep/quic: marshal key: %w", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		return tls.Certificate{}, fmt.Errorf("afep/quic: write key: %w", err)
	}
	if err := os.WriteFile(certFile, certPEM, 0o644); err != nil {
		return tls.Certificate{}, fmt.Errorf("afep/quic: write cert: %w", err)
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}

// ServerTLS builds the listener-side TLS config (TLS 1.3 mandatory in QUIC).
func ServerTLS(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{ALPN},
	}
}

// ClientTLS builds the dialer-side config. InsecureSkipVerify keeps the
// scaffold runnable without a CA; pinning (or mTLS) is the production path.
func ClientTLS() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, // dev scaffold: see ARCHITECTURE.md gaps
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{ALPN},
	}
}
