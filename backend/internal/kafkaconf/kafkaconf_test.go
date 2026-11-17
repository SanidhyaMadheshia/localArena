package kafkaconf

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func writeCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoad(t *testing.T) {
	ca := writeCA(t)
	bad := filepath.Join(t.TempDir(), "bad.crt")
	_ = os.WriteFile(bad, []byte("not a pem"), 0o600)
	const secret = "s3cr3t-pass"
	tests := []struct {
		name    string
		env     map[string]string
		wantN   int
		wantErr string
	}{
		{"empty is plaintext", nil, 0, ""},
		{"explicit plaintext", map[string]string{"KAFKA_SECURITY_PROTOCOL": "plaintext"}, 0, ""},
		{"ssl only", map[string]string{"KAFKA_SECURITY_PROTOCOL": "SSL"}, 1, ""},
		{"ssl with ca", map[string]string{"KAFKA_SECURITY_PROTOCOL": "SSL", "KAFKA_CA_FILE": ca}, 1, ""},
		{"sasl default mech", map[string]string{"KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_USERNAME": "u", "KAFKA_PASSWORD": secret, "KAFKA_CA_FILE": ca}, 2, ""},
		{"sasl 256", map[string]string{"KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_SASL_MECHANISM": "SCRAM-SHA-256", "KAFKA_USERNAME": "u", "KAFKA_PASSWORD": secret}, 2, ""},
		{"sasl missing user", map[string]string{"KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_PASSWORD": secret}, 0, "requires KAFKA_USERNAME"},
		{"sasl missing password", map[string]string{"KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_USERNAME": "u"}, 0, "requires KAFKA_USERNAME"},
		{"unknown protocol", map[string]string{"KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT"}, 0, "unknown KAFKA_SECURITY_PROTOCOL"},
		{"unknown mechanism", map[string]string{"KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_SASL_MECHANISM": "PLAIN", "KAFKA_USERNAME": "u", "KAFKA_PASSWORD": secret}, 0, "unsupported KAFKA_SASL_MECHANISM"},
		{"missing ca file", map[string]string{"KAFKA_SECURITY_PROTOCOL": "SSL", "KAFKA_CA_FILE": "/nonexistent/ca.crt"}, 0, "read KAFKA_CA_FILE"},
		{"invalid ca pem", map[string]string{"KAFKA_SECURITY_PROTOCOL": "SSL", "KAFKA_CA_FILE": bad}, 0, "no valid PEM"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := Load(func(k string) string { return tc.env[k] })
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				if strings.Contains(err.Error(), secret) {
					t.Fatal("error leaks password")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(opts) != tc.wantN {
				t.Fatalf("got %d opts, want %d", len(opts), tc.wantN)
			}
			// The options must be accepted by franz-go (no network is dialed).
			cl, err := kgo.NewClient(append(opts, kgo.SeedBrokers("127.0.0.1:1"))...)
			if err != nil {
				t.Fatal(err)
			}
			cl.Close()
		})
	}
}
