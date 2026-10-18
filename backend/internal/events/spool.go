package events

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// spool is an append-only, bounded, crash-tolerant log of undelivered records.
//
// Frame: crc32c(4) | length(4) | payload, payload = uvarint-prefixed topic, key,
// event_id header, then value. A torn last frame (crash mid-write) or a bad CRC
// ends reading of that file: the record was never acknowledged as spooled.
//
// Draining renames the active file to spool-<ns>.replay and starts a new one,
// so new failures can be spooled while old ones replay. Replay files left by a
// crash are picked up on the next drain.
type spool struct {
	dir string
	max int64

	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
	cur  int64 // bytes in the active file
	held int64 // bytes in replay files not yet deleted
}

var (
	errSpoolFull = errors.New("event spool full")
	crcTable     = crc32.MakeTable(crc32.Castagnoli)
)

const activeName = "spool.log"

func openSpool(dir string, max int64) (*spool, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	s := &spool{dir: dir, max: max}
	if err := s.openActive(); err != nil {
		return nil, err
	}
	for _, f := range s.replayFiles() {
		if st, err := os.Stat(f); err == nil {
			s.held += st.Size()
		}
	}
	return s, nil
}

func (s *spool) openActive() error {
	f, err := os.OpenFile(filepath.Join(s.dir, activeName), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	s.f, s.w, s.cur = f, bufio.NewWriter(f), st.Size()
	return nil
}

func (s *spool) size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur + s.held
}

func (s *spool) hasPending() bool { return len(s.replayFiles()) > 0 }

func (s *spool) replayFiles() []string {
	m, _ := filepath.Glob(filepath.Join(s.dir, "spool-*.replay"))
	slices.Sort(m)
	return m
}

func encode(r *kgo.Record) []byte {
	var id []byte
	for _, h := range r.Headers {
		if h.Key == "event_id" {
			id = h.Value
		}
	}
	b := make([]byte, 8, 8+len(r.Topic)+len(r.Key)+len(id)+len(r.Value)+24)
	for _, f := range [][]byte{[]byte(r.Topic), r.Key, id} {
		b = binary.AppendUvarint(b, uint64(len(f)))
		b = append(b, f...)
	}
	b = binary.AppendVarint(b, r.Timestamp.UnixMilli())
	b = append(b, r.Value...)
	binary.BigEndian.PutUint32(b[4:8], uint32(len(b)-8)) // #nosec G115 -- frames are far below 4 GiB
	binary.BigEndian.PutUint32(b[0:4], crc32.Checksum(b[4:], crcTable))
	return b
}

func decode(payload []byte) (*kgo.Record, error) {
	var fields [3][]byte
	for i := range fields {
		n, k := binary.Uvarint(payload)
		if k <= 0 || uint64(len(payload)-k) < n { // #nosec G115 -- bounded by len(payload) checked above
			return nil, errors.New("corrupt spool record")
		}
		fields[i], payload = payload[k:k+int(n)], payload[k+int(n):] // #nosec G115 -- bounded by len(payload)
	}
	ms, k := binary.Varint(payload)
	if k <= 0 {
		return nil, errors.New("corrupt spool record")
	}
	r := &kgo.Record{Topic: string(fields[0]), Key: fields[1], Value: payload[k:], Timestamp: time.UnixMilli(ms)}
	if len(fields[2]) > 0 {
		r.Headers = []kgo.RecordHeader{{Key: "event_id", Value: fields[2]}}
	}
	return r, nil
}

// append writes one record and flushes it to the OS. fsync per record would
// cost too much under a burst; the window of loss on host crash is documented.
func (s *spool) append(r *kgo.Record) error {
	b := encode(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur+s.held+int64(len(b)) > s.max {
		return errSpoolFull
	}
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	s.cur += int64(len(b))
	return nil
}

// drain rotates the active file and sends every spooled record through send in
// batches. On error, the unsent remainder is written back to the active spool.
func (s *spool) drain(send func([]*kgo.Record) error) (int, error) {
	s.mu.Lock()
	if s.cur > 0 {
		_ = s.w.Flush()
		_ = s.f.Sync()
		_ = s.f.Close()
		name := filepath.Join(s.dir, fmt.Sprintf("spool-%020d.replay", time.Now().UnixNano()))
		if err := os.Rename(filepath.Join(s.dir, activeName), name); err != nil {
			s.mu.Unlock()
			return 0, err
		}
		s.held += s.cur
		if err := s.openActive(); err != nil {
			s.mu.Unlock()
			return 0, err
		}
	}
	s.mu.Unlock()

	total := 0
	for _, file := range s.replayFiles() {
		recs, err := readAll(file)
		if err != nil {
			return total, err
		}
		for i := 0; i < len(recs); i += 500 {
			batch := recs[i:min(i+500, len(recs))]
			if err := send(batch); err != nil {
				for _, r := range recs[i:] { // keep what was not confirmed
					_ = s.append(r)
				}
				s.forget(file)
				return total, err
			}
			total += len(batch)
		}
		s.forget(file)
	}
	return total, nil
}

func (s *spool) forget(file string) {
	st, err := os.Stat(file)
	if err == nil && os.Remove(file) == nil {
		s.mu.Lock()
		s.held -= st.Size()
		s.mu.Unlock()
	}
}

func readAll(file string) ([]*kgo.Record, error) {
	f, err := os.Open(file) // #nosec G304 -- path comes from our own spool directory glob
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	r := bufio.NewReader(f)
	var out []*kgo.Record
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return out, nil // EOF or torn header: end of valid data
		}
		n := binary.BigEndian.Uint32(hdr[4:])
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return out, nil // torn frame from a crash mid-append
		}
		crc := crc32.Update(crc32.Checksum(hdr[4:], crcTable), crcTable, payload)
		if crc != binary.BigEndian.Uint32(hdr[:4]) {
			return out, nil // corruption: stop rather than emit garbage
		}
		rec, err := decode(payload)
		if err != nil {
			return out, nil
		}
		out = append(out, rec)
	}
}

func (s *spool) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.w.Flush()
	_ = s.f.Sync()
	return s.f.Close()
}
