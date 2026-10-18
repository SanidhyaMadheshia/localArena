// Command eventcat prints LocalArena Kafka events as JSON lines (debug tool).
//
//	eventcat -topic game.economy-events.v1 [-from-start] [-n 10] [-brokers localhost:29092]
//	eventcat -topic game.match-events.v1 -from-start -idle 5s -replay MatchCompleted,PlayerScored
//	    re-produces matching records byte-for-byte (same key, value, event_id): a duplicate-delivery drill
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/SanidhyaMadheshia/localArena/backend/gen/events/v1"
)

var failedReplays atomic.Int64

func main() {
	brokers := flag.String("brokers", "localhost:29092,localhost:39092,localhost:49092", "Kafka brokers")
	topic := flag.String("topic", "game.match-events.v1", "topic (comma-separated for several)")
	fromStart := flag.Bool("from-start", false, "read from the beginning")
	n := flag.Int("n", 0, "stop after n records (0 = forever)")
	idle := flag.Duration("idle", 0, "stop after this long without records (0 = never)")
	replay := flag.String("replay", "", "comma-separated payload kinds to re-produce verbatim (e.g. MatchCompleted)")
	replayMax := flag.Int("replay-max", 0, "stop after re-producing this many records (0 = no limit)")
	flag.Parse()
	off := kgo.NewOffset().AtEnd()
	if *fromStart {
		off = kgo.NewOffset().AtStart()
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(strings.Split(*topic, ",")...), kgo.ConsumeResetOffset(off))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer cl.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	// Replay mode re-produces into the topic it reads, so it stops at the end
	// offsets that existed at start; otherwise it would chase its own output forever.
	var stopAt map[string]map[int32]int64
	if *replay != "" {
		ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, strings.Split(*topic, ",")...)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		stopAt = map[string]map[int32]int64{}
		ends.Each(func(o kadm.ListedOffset) {
			if stopAt[o.Topic] == nil {
				stopAt[o.Topic] = map[int32]int64{}
			}
			stopAt[o.Topic][o.Partition] = o.Offset
		})
	}
	seen, replayed := 0, 0
	kinds := map[string]bool{}
	for _, k := range strings.Split(*replay, ",") {
		if k != "" {
			kinds[k] = true
		}
	}
	defer func() {
		if len(kinds) > 0 {
			_ = cl.Flush(context.Background())
			fmt.Fprintf(os.Stderr, "replayed %d records (%d failed)\n", replayed, failedReplays.Load())
		}
	}()
	for {
		pctx, cancel := ctx, context.CancelFunc(func() {})
		if *idle > 0 {
			pctx, cancel = context.WithTimeout(ctx, *idle)
		}
		fs := cl.PollFetches(pctx)
		cancel()
		if len(fs.Records()) == 0 && pctx.Err() != nil {
			return // idle timeout (or interrupt) with nothing left to print
		}
		for _, r := range fs.Records() {
			if stopAt != nil && r.Offset >= stopAt[r.Topic][r.Partition] {
				continue // our own re-produced copies (or newer traffic)
			}
			var env eventsv1.Envelope
			body := "<undecodable>"
			if proto.Unmarshal(r.Value, &env) == nil {
				b, _ := protojson.Marshal(&env)
				body = string(b)
				if env.Payload != nil && kinds[strings.TrimPrefix(fmt.Sprintf("%T", env.Payload), "*eventsv1.Envelope_")] {
					if *replayMax > 0 && replayed >= *replayMax {
						continue
					}
					// Async produce (batched by the client); errors are counted, Flush waits at the end.
					dup := &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value, Headers: r.Headers, Timestamp: time.Now()}
					replayed++
					cl.Produce(ctx, dup, func(_ *kgo.Record, err error) {
						if err != nil {
							failedReplays.Add(1)
						}
					})
					continue
				}
			}
			if len(kinds) > 0 {
				continue
			}
			fmt.Printf(`{"topic":%q,"partition":%d,"offset":%d,"key":%q,"ts":%q,"event":%s}`+"\n",
				r.Topic, r.Partition, r.Offset, r.Key, r.Timestamp.Format(time.RFC3339Nano), body)
			if seen++; *n > 0 && seen >= *n {
				return
			}
		}
	}
}
