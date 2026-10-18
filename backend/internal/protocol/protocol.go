// Package protocol defines LocalArena wire messages, version 1 (JSON over WebSocket).
//
// Client -> server:
//
//	{"t":"input","seq":42,"dx":1,"dy":0,"ct":1712345678901,"st":120}
//
// Server -> client:
//
//	{"t":"welcome","v":1,"id":7,"match":"m-1","tickHz":20,"w":1000,"h":1000}
//	{"t":"snap","tick":123,"ack":42,"players":[{"id":7,"x":10,"y":20,"s":3}],"items":[[1,2]]}
//
// "ack" is the highest input sequence the server has applied for the receiving
// player; clients use it for RTT measurement and (later) reconciliation.
// "ct" is echoed only for diagnostics and never trusted for ordering.
// "st" lets the server stop feeding a client that is behind (kernel socket
// buffers would otherwise queue seconds of stale snapshots; see Experiment 001).
package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Version is bumped on any incompatible change (see adr/0002).
const Version = 1

// MaxClientMessage bounds inbound frame size; inputs are tiny.
const MaxClientMessage = 256

// Message types.
const (
	TypeInput   = "input"
	TypeWelcome = "welcome"
	TypeSnap    = "snap"
)

// Input is the only client-originated gameplay message.
type Input struct {
	T   string `json:"t"`
	Seq uint32 `json:"seq"`
	Dx  int8   `json:"dx"`
	Dy  int8   `json:"dy"`
	CT  int64  `json:"ct,omitempty"` // client clock, ms; diagnostics only
	ST  uint64 `json:"st,omitempty"` // last snapshot tick the client processed; optional (v1-compatible)
}

// ErrBadMessage is returned for malformed or unknown client messages.
var ErrBadMessage = errors.New("protocol: bad message")

// DecodeInput parses and validates a client frame.
func DecodeInput(b []byte) (Input, error) {
	var in Input
	if len(b) > MaxClientMessage {
		return in, fmt.Errorf("%w: %d bytes", ErrBadMessage, len(b))
	}
	if err := json.Unmarshal(b, &in); err != nil {
		return in, fmt.Errorf("%w: %v", ErrBadMessage, err)
	}
	if in.T != TypeInput || in.Seq == 0 {
		return in, fmt.Errorf("%w: type=%q seq=%d", ErrBadMessage, in.T, in.Seq)
	}
	return in, nil
}

// Welcome is sent once after a session joins a match.
type Welcome struct {
	T      string `json:"t"`
	V      int    `json:"v"`
	ID     uint32 `json:"id"`
	Match  string `json:"match"`
	TickHz int    `json:"tickHz"`
	W      int32  `json:"w"`
	H      int32  `json:"h"`
}

// PlayerState is the public view of one player.
type PlayerState struct {
	ID uint32 `json:"id"`
	X  int32  `json:"x"`
	Y  int32  `json:"y"`
	S  uint32 `json:"s"`
}

// Snapshot is the per-tick world view. Players/Items are pre-encoded once per
// tick and shared by every recipient; only Ack differs per player.
type Snapshot struct {
	T       string          `json:"t"`
	Tick    uint64          `json:"tick"`
	Ack     uint32          `json:"ack"`
	Players json.RawMessage `json:"players"`
	Items   json.RawMessage `json:"items"`
}
