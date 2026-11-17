// Package kafkaconf builds franz-go client options (TLS, SASL/SCRAM) from
// environment variables, so every Kafka client in the repo authenticates the
// same way. With no variables set it returns no options (plaintext, as in Compose).
//
//	KAFKA_SECURITY_PROTOCOL  PLAINTEXT (default) | SSL | SASL_SSL
//	KAFKA_SASL_MECHANISM     SCRAM-SHA-512 (default) | SCRAM-SHA-256
//	KAFKA_USERNAME           required for SASL_SSL
//	KAFKA_PASSWORD           required for SASL_SSL
//	KAFKA_CA_FILE            PEM bundle to trust; empty = system roots
package kafkaconf

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// FromEnv reads the KAFKA_* variables from the process environment.
func FromEnv() ([]kgo.Opt, error) { return Load(os.Getenv) }

// Load is FromEnv with an injectable lookup. Errors never contain the password.
func Load(getenv func(string) string) ([]kgo.Opt, error) {
	proto := strings.ToUpper(strings.TrimSpace(getenv("KAFKA_SECURITY_PROTOCOL")))
	if proto == "" {
		proto = "PLAINTEXT"
	}
	switch proto {
	case "PLAINTEXT":
		return nil, nil
	case "SSL", "SASL_SSL":
	default:
		return nil, fmt.Errorf("kafkaconf: unknown KAFKA_SECURITY_PROTOCOL %q (want PLAINTEXT, SSL or SASL_SSL)", proto)
	}

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if ca := strings.TrimSpace(getenv("KAFKA_CA_FILE")); ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("kafkaconf: read KAFKA_CA_FILE: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafkaconf: KAFKA_CA_FILE %q contains no valid PEM certificate", ca)
		}
		tlsCfg.RootCAs = pool
	}
	opts := []kgo.Opt{kgo.DialTLSConfig(tlsCfg)}
	if proto == "SSL" {
		return opts, nil
	}

	user, pass := getenv("KAFKA_USERNAME"), getenv("KAFKA_PASSWORD")
	if user == "" || pass == "" {
		return nil, errors.New("kafkaconf: SASL_SSL requires KAFKA_USERNAME and KAFKA_PASSWORD")
	}
	auth := scram.Auth{User: user, Pass: pass}
	mech := strings.ToUpper(strings.TrimSpace(getenv("KAFKA_SASL_MECHANISM")))
	switch mech {
	case "", "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(auth.AsSha512Mechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(auth.AsSha256Mechanism()))
	default:
		return nil, fmt.Errorf("kafkaconf: unsupported KAFKA_SASL_MECHANISM %q (want SCRAM-SHA-512 or SCRAM-SHA-256)", mech)
	}
	return opts, nil
}
