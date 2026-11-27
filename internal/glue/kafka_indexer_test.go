package glue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeReader serves a fixed list of messages, then blocks until cancelled.
type fakeReader struct {
	mu        sync.Mutex
	msgs      []kafka.Message
	next      int
	committed map[int]int64
	commitErr error
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if r.next < len(r.msgs) {
		m := r.msgs[r.next]
		r.next++
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.commitErr != nil {
		return r.commitErr
	}
	for _, m := range msgs {
		r.committed[m.Partition] = m.Offset
	}
	return nil
}

func (r *fakeReader) ReadLag(context.Context) (int64, error) { return 0, nil }
func (r *fakeReader) Close() error                          { return nil }

func (r *fakeReader) committedOffset(p int) (int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o, ok := r.committed[p]
	return o, ok
}

type fakeDLQ struct {
	mu    sync.Mutex
	fails int
	got   []kafka.Message
}

func (w *fakeDLQ) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fails > 0 {
		w.fails--
		return errors.New("dlq unavailable")
	}
	w.got = append(w.got, msgs...)
	return nil
}

func msgs(n int, partition int) []kafka.Message {
	out := make([]kafka.Message, n)
	for i := range out {
		out[i] = kafka.Message{Topic: "t", Partition: partition, Offset: int64(i), Key: []byte{byte(i)}}
	}
	return out
}

func runDaemon(t *testing.T, r *fakeReader, dlq messageWriter, h MessageHandler, opts DaemonOptions, until func() bool) {
	t.Helper()
	d := newDaemon(r, "t", h)
	d.dlq = dlq
	opts.CommitInterval = 10 * time.Millisecond
	opts.RetryBackoff = time.Millisecond
	d.SetOptions(opts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- d.Start(ctx) }()
	require.Eventually(t, until, 5*time.Second, 5*time.Millisecond)
	cancel()
	<-done
}

// Regression: a failed message used to be skipped (the next fetch moved on)
// and implicitly committed by later offsets.
func TestDaemonRetriesFailedMessageInPlace(t *testing.T) {
	r := &fakeReader{msgs: msgs(5, 0), committed: map[int]int64{}}
	var attempts atomic.Int32
	h := func(_ context.Context, m kafka.Message) error {
		if m.Offset == 2 && attempts.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	}
	runDaemon(t, r, nil, h, DaemonOptions{Workers: 2, MaxAttempts: 5}, func() bool {
		o, ok := r.committedOffset(0)
		return ok && o == 4
	})
	assert.Equal(t, int32(3), attempts.Load(), "offset 2 retried until it succeeded")
}

func TestDaemonDeadLetters(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantCalls   int32
		dlqFailures int
	}{
		{"transient error exhausts retries", errors.New("boom"), 3, 0},
		{"permanent error skips retries", Permanent(errors.New("bad payload")), 1, 0},
		{"DLQ outage holds the offset until the write succeeds", errors.New("boom"), 3, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &fakeReader{msgs: msgs(3, 0), committed: map[int]int64{}}
			dlq := &fakeDLQ{fails: tt.dlqFailures}
			var calls atomic.Int32
			h := func(_ context.Context, m kafka.Message) error {
				if m.Offset == 1 {
					calls.Add(1)
					return tt.err
				}
				return nil
			}
			runDaemon(t, r, dlq, h, DaemonOptions{Workers: 1, MaxAttempts: 3}, func() bool {
				o, ok := r.committedOffset(0)
				return ok && o == 2
			})
			assert.Equal(t, tt.wantCalls, calls.Load())
			require.Len(t, dlq.got, 1)
			assert.Equal(t, []byte{1}, dlq.got[0].Key, "the failed message (offset 1) was dead-lettered")
			assert.Equal(t, "yase-error", dlq.got[0].Headers[0].Key)
		})
	}
}

func TestDaemonCommitsOnlyAfterCheckpoint(t *testing.T) {
	r := &fakeReader{msgs: msgs(4, 0), committed: map[int]int64{}}
	var checkpoints atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	opts := DaemonOptions{Workers: 2, Checkpoint: func(context.Context) error {
		checkpoints.Add(1)
		if fail.Load() {
			return errors.New("disk full")
		}
		return nil
	}}
	h := func(context.Context, kafka.Message) error { return nil }

	d := newDaemon(r, "t", h)
	opts.CommitInterval = 10 * time.Millisecond
	d.SetOptions(opts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- d.Start(ctx) }()

	require.Eventually(t, func() bool { return checkpoints.Load() >= 2 }, 5*time.Second, 5*time.Millisecond)
	_, ok := r.committedOffset(0)
	assert.False(t, ok, "nothing committed while checkpoints fail")

	fail.Store(false)
	require.Eventually(t, func() bool { o, ok := r.committedOffset(0); return ok && o == 3 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	<-done
}

func TestOffsetTrackerCommitsContiguousPrefixOnly(t *testing.T) {
	tr := newOffsetTracker()
	fs := make([]*inflight, 4)
	for i := range fs {
		fs[i] = tr.add(kafka.Message{Partition: 0, Offset: int64(i)})
	}
	tr.markDone(fs[0])
	tr.markDone(fs[2])
	ready := tr.ready()
	require.Len(t, ready, 1)
	assert.Equal(t, int64(0), ready[0].Offset, "offset 2 done but 1 is not: commit stops at 0")

	tr.markDone(fs[1])
	ready = tr.ready()
	require.Len(t, ready, 1)
	assert.Equal(t, int64(2), ready[0].Offset)
}
