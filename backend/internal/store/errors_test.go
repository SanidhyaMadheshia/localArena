package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsUnavailable(t *testing.T) {
	cases := map[string]bool{"08P01": true, "08006": true, "57P01": true, "25006": true, "23505": false, "40P01": false}
	for code, want := range cases {
		err := fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: code})
		if got := IsUnavailable(err); got != want {
			t.Errorf("%s: got %v want %v", code, got, want)
		}
	}
	if IsUnavailable(errors.New("plain")) {
		t.Error("plain error classified unavailable")
	}
}
