package webhook

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSignVerify(t *testing.T) {
	sec, old := []byte("current-secret-current-secret-xx"), []byte("previous-secret-previous-secret-")
	body := []byte(`{"match_id":"m1"}`)
	now := time.Unix(1_800_000_000, 0)
	h := Sign(sec, now, "d1", body)
	if err := Verify(h, "d1", body, now.Add(time.Minute), sec); err != nil {
		t.Fatal(err)
	}
	cases := map[string]error{
		"tampered body":  Verify(h, "d1", []byte(`{"match_id":"m2"}`), now, sec),
		"other delivery": Verify(h, "d2", body, now, sec),
		"wrong secret":   Verify(h, "d1", body, now, []byte("nope")),
		"stale":          Verify(h, "d1", body, now.Add(6*time.Minute), sec),
		"future":         Verify(h, "d1", body, now.Add(-6*time.Minute), sec),
		"no v1":          Verify(strings.Split(h, ",")[0], "d1", body, now, sec),
		"garbage":        Verify("t=abc,v1=00", "d1", body, now, sec),
	}
	for name, err := range cases {
		if err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if !errors.Is(cases["stale"], ErrStale) {
		t.Error("stale must be ErrStale")
	}
	// Rotation: signed with the old secret, receiver knows both.
	if err := Verify(Sign(old, now, "d3", body), "d3", body, now, sec, old); err != nil {
		t.Fatalf("rotation: %v", err)
	}
}
