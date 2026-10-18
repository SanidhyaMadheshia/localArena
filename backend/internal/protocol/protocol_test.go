package protocol

import (
	"errors"
	"strings"
	"testing"
)

func TestDecodeInput(t *testing.T) {
	cases := []struct {
		name string
		in   string
		ok   bool
	}{
		{"valid", `{"t":"input","seq":1,"dx":1,"dy":-1}`, true},
		{"zero seq", `{"t":"input","seq":0,"dx":1}`, false},
		{"wrong type", `{"t":"snap","seq":1}`, false},
		{"not json", `hello`, false},
		{"dx overflow", `{"t":"input","seq":1,"dx":300}`, false},
		{"oversized", `{"t":"input","seq":1,"pad":"` + strings.Repeat("x", 300) + `"}`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := DecodeInput([]byte(c.in))
			if c.ok != (err == nil) {
				t.Fatalf("err = %v, want ok=%v", err, c.ok)
			}
			if err != nil && !errors.Is(err, ErrBadMessage) {
				t.Fatalf("error not wrapped: %v", err)
			}
		})
	}
}

func FuzzDecodeInput(f *testing.F) {
	f.Add([]byte(`{"t":"input","seq":1,"dx":1,"dy":0}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		in, err := DecodeInput(b)
		if err == nil && (in.Seq == 0 || in.T != TypeInput) {
			t.Fatalf("accepted invalid input %+v", in)
		}
	})
}
