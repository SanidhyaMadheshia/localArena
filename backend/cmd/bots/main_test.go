package main

import (
	"math"
	"testing"
)

func TestHistQuantile(t *testing.T) {
	// 100 samples: 50 in (0,1], 40 in (1,2], 10 in (2,4]
	h := hist{bounds: []float64{1, 2, 4, math.Inf(1)}, counts: []float64{50, 90, 100, 100}, total: 100}
	cases := map[float64]float64{0.5: 1, 0.7: 1.5, 0.95: 3}
	for q, want := range cases {
		if got := h.quantile(q); math.Abs(got-want) > 1e-9 {
			t.Errorf("q%.2f = %v, want %v", q, got, want)
		}
	}
	if (hist{}).quantile(.5) != 0 {
		t.Error("empty histogram should be 0")
	}
	tail := hist{bounds: []float64{1, math.Inf(1)}, counts: []float64{1, 10}, total: 10}
	if got := tail.quantile(.99); got != 1 {
		t.Errorf("+Inf bucket quantile = %v, want highest finite bound 1", got)
	}
}

func TestWSURL(t *testing.T) {
	for in, want := range map[string]string{
		"http://localhost:8080":  "ws://localhost:8080/ws",
		"https://arena.example/": "wss://arena.example/ws",
	} {
		if got := wsURL(in); got != want {
			t.Errorf("wsURL(%q) = %q, want %q", in, got, want)
		}
	}
}
