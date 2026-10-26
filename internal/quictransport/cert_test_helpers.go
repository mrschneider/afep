package quictransport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
)

func x509leaf(cert tls.Certificate) (*x509.Certificate, error) {
	if len(cert.Certificate) == 0 {
		return nil, fmt.Errorf("no certificate")
	}
	return x509.ParseCertificate(cert.Certificate[0])
}
