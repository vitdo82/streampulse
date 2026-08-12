package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
)

// TLSOptions configures TLS and mutual TLS for broker connections. An empty
// (zero-value) TLSOptions leaves the client in plaintext mode.
type TLSOptions struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
}

// buildTLSConfig loads the *tls.Config described by opts, or nil when TLS is
// not enabled. CAFile populates RootCAs; CertFile+KeyFile enable mTLS client
// authentication. Error messages never include key material.
func buildTLSConfig(opts TLSOptions) (*tls.Config, error) {
	if !opts.Enabled {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: opts.InsecureSkipVerify,
	}

	if opts.CAFile != "" {
		pemBytes, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("CA file %s: no valid certificates found", opts.CAFile)
		}
		cfg.RootCAs = pool
	}

	if opts.CertFile != "" || opts.KeyFile != "" {
		if opts.CertFile == "" || opts.KeyFile == "" {
			return nil, fmt.Errorf("mTLS requires both cert_file and key_file")
		}
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	if opts.InsecureSkipVerify {
		slog.Warn("kafka: TLS InsecureSkipVerify is enabled; broker certificates will not be verified")
	}
	return cfg, nil
}
