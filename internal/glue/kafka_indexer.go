package glue

import (
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/efathom/yase/pkg/metrics"
	"github.com/segmentio/kafka-go"
)

// ErrPermanent marks a handler failure that retrying cannot fix (malformed
// payload, unknown collection). Such messages go straight to the DLQ.
var ErrPermanent = errors.New("permanent message failure")

// Permanent wraps err so the daemon dead-letters the message without retrying.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err}
}

type permanentError struct{ err error }

func (e *permanentError) Error() string        { return e.err.Error() }
func (e *permanentError) Unwrap() error        { return e.err }
func (e *permanentError) Is(target error) bool { return target == ErrPermanent }

// MessageHandler processes a single Kafka message. Returning an error causes
// an in-place retry (with backoff) up to MaxAttempts, then dead-lettering.
type MessageHandler func(ctx context.Context, msg kafka.Message) error

// messageReader is the subset of *kafka.Reader the daemon uses.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	ReadLag(ctx context.Context) (int64, error)
	Close() error
}

// messageWriter is the subset of *kafka.Writer used for the DLQ.
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// DaemonOptions tunes the consumer.
type DaemonOptions struct {
	// Workers is the number of concurrent handlers. Messages with the same
	// key (URL) always go to the same worker, preserving per-document order.
	Workers int
	// MaxAttempts is how many times a failing message is tried before it is
	// dead-lettered (default 3).
	MaxAttempts int
	// RetryBackoff is the initial delay between attempts; it doubles each time.
	RetryBackoff time.Duration
	// CommitInterval is how often processed offsets are committed.
	CommitInterval time.Duration
	// Checkpoint, when set, is called before every offset commit and must
	// durably persist everything processed so far. Offsets are only committed
	// after it succeeds, so a crash re-delivers — never loses — messages whose
	// effects were not yet durable.
	Checkpoint func(ctx context.Context) error
}

func (o *DaemonOptions) normalize() {
	if o.Workers <= 0 {
		o.Workers = 4
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 3
	}
	if o.RetryBackoff <= 0 {
		o.RetryBackoff = 500 * time.Millisecond
	}
	if o.CommitInterval <= 0 {
		o.CommitInterval = 5 * time.Second
	}
}

// IndexingDaemon consumes from Kafka with at-least-once semantics: an offset
// is committed only after its message (and every earlier message of the same
// partition) was handled or dead-lettered, and — when a Checkpoint is
// configured — after the index state is durable.
type IndexingDaemon struct {
	reader  messageReader
	handler MessageHandler
	topic   string
	dlq     messageWriter
	opts    DaemonOptions

	tracker   *offsetTracker
	closeOnce sync.Once
	closeErr  error
}

// NewIndexingDaemon creates a Kafka consumer with manual commits.
func NewIndexingDaemon(brokers []string, topic, groupID string, handler MessageHandler) *IndexingDaemon {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024, // 10MB
		MaxWait:        500 * time.Millisecond,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0, // manual commit only
	})
	return newDaemon(r, topic, handler)
}

func newDaemon(r messageReader, topic string, handler MessageHandler) *IndexingDaemon {
	d := &IndexingDaemon{reader: r, handler: handler, topic: topic, tracker: newOffsetTracker()}
	d.opts.normalize()
	return d
}

// SetDLQ attaches a dead-letter topic writer. Without one, messages that
// exhaust their retries are logged and skipped.
func (d *IndexingDaemon) SetDLQ(w *kafka.Writer) {
	if w != nil {
		d.dlq = w
	}
}

// SetOptions configures concurrency, retries and checkpointed commits.
// Call before Start.
func (d *IndexingDaemon) SetOptions(o DaemonOptions) {
	o.normalize()
	d.opts = o
}

// Start runs the consumer until ctx is cancelled. On shutdown it drains
// in-flight messages and performs a final checkpoint + commit.
func (d *IndexingDaemon) Start(ctx context.Context) error {
	go d.reportLag(ctx)

	queues := make([]chan *inflight, d.opts.Workers)
	var wg sync.WaitGroup
	for i := range queues {
		queues[i] = make(chan *inflight, 16)
		wg.Add(1)
		go func(q <-chan *inflight) {
			defer wg.Done()
			for f := range q {
				if d.process(ctx, f.msg) {
					d.tracker.markDone(f)
				}
			}
		}(queues[i])
	}

	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		t := time.NewTicker(d.opts.CommitInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				d.commit(ctx)
			}
		}
	}()

	for {
		msg, err := d.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Warn("indexer: fetch error (transient, retrying)", "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
			continue
		}
		f := d.tracker.add(msg)
		select {
		case queues[workerFor(msg, len(queues))] <- f:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}

	for _, q := range queues {
		close(q)
	}
	wg.Wait()
	<-commitDone

	// Final commit of everything that completed, with a fresh deadline.
	finalCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	d.commit(finalCtx)
	cancel()
	return d.Close()
}

func workerFor(msg kafka.Message, n int) int {
	h := fnv.New32a()
	if len(msg.Key) > 0 {
		h.Write(msg.Key)
	} else {
		h.Write([]byte{byte(msg.Partition), byte(msg.Partition >> 8)})
	}
	return int(h.Sum32() % uint32(n))
}

// process handles one message with retries. It returns true when the message
// is finished (succeeded, or dead-lettered/skipped) and may be committed;
// false when shutdown interrupted it, so it must be re-delivered.
func (d *IndexingDaemon) process(ctx context.Context, msg kafka.Message) bool {
	backoff := d.opts.RetryBackoff
	var err error
	for attempt := 1; attempt <= d.opts.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return false
		}
		if err = d.handler(ctx, msg); err == nil {
			metrics.IndexedMessagesTotal.WithLabelValues("ok").Inc()
			return true
		}
		if ctx.Err() != nil {
			return false // cancelled mid-handle: let it be re-delivered
		}
		if errors.Is(err, ErrPermanent) {
			break
		}
		slog.Warn("indexer: handler error, retrying",
			"partition", msg.Partition, "offset", msg.Offset, "attempt", attempt, "error", err)
		if attempt < d.opts.MaxAttempts {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(backoff):
			}
			backoff *= 2
		}
	}
	return d.deadLetter(ctx, msg, err)
}

// deadLetter writes msg to the DLQ, retrying until it succeeds: a message is
// never committed (and therefore never lost) while the DLQ is unavailable.
func (d *IndexingDaemon) deadLetter(ctx context.Context, msg kafka.Message, cause error) bool {
	metrics.IndexedMessagesTotal.WithLabelValues("dead_lettered").Inc()
	slog.Error("indexer: dead-lettering message",
		"partition", msg.Partition, "offset", msg.Offset, "key", string(msg.Key), "error", cause)
	if d.dlq == nil {
		return true
	}
	reason := ""
	if cause != nil {
		reason = cause.Error()
	}
	dl := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Time:  msg.Time,
		Headers: append(append([]kafka.Header(nil), msg.Headers...),
			kafka.Header{Key: "yase-error", Value: []byte(reason)},
			kafka.Header{Key: "yase-source-topic", Value: []byte(msg.Topic)}),
	}
	backoff := d.opts.RetryBackoff
	for {
		err := d.dlq.WriteMessages(ctx, dl)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		slog.Error("indexer: DLQ write failed; holding offset", "offset", msg.Offset, "error", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// commit checkpoints (if configured) and commits the highest contiguous
// completed offset of each partition.
func (d *IndexingDaemon) commit(ctx context.Context) {
	msgs := d.tracker.ready()
	if len(msgs) == 0 {
		return
	}
	if d.opts.Checkpoint != nil {
		if err := d.opts.Checkpoint(ctx); err != nil {
			slog.Error("indexer: checkpoint failed; offsets not committed", "error", err)
			d.tracker.requeue(msgs)
			return
		}
	}
	if err := d.reader.CommitMessages(ctx, msgs...); err != nil {
		slog.Error("indexer: commit error", "error", err)
		d.tracker.requeue(msgs)
		return
	}
	d.tracker.markCommitted(msgs)
}

// reportLag periodically publishes the consumer lag to Prometheus.
func (d *IndexingDaemon) reportLag(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if lag, err := d.reader.ReadLag(ctx); err == nil {
				metrics.KafkaConsumerLag.WithLabelValues(d.topic, "all").Set(float64(lag))
			}
		}
	}
}

// Close shuts down the Kafka reader cleanly. Idempotent.
func (d *IndexingDaemon) Close() error {
	d.closeOnce.Do(func() {
		d.closeErr = d.reader.Close()
	})
	return d.closeErr
}

// ── offset tracking ──

type inflight struct {
	msg  kafka.Message
	done bool
}

// offsetTracker records fetched messages per partition in fetch (offset)
// order and yields, per partition, the last message of the completed prefix.
type offsetTracker struct {
	mu        sync.Mutex
	pending   map[int][]*inflight
	readyMsg  map[int]kafka.Message // uncommitted commit candidate per partition
	committed map[int]int64         // highest committed offset per partition
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{
		pending:   make(map[int][]*inflight),
		readyMsg:  make(map[int]kafka.Message),
		committed: make(map[int]int64),
	}
}

func (t *offsetTracker) add(msg kafka.Message) *inflight {
	f := &inflight{msg: msg}
	t.mu.Lock()
	t.pending[msg.Partition] = append(t.pending[msg.Partition], f)
	t.mu.Unlock()
	return f
}

func (t *offsetTracker) markDone(f *inflight) {
	t.mu.Lock()
	f.done = true
	t.mu.Unlock()
}

// ready pops each partition's completed prefix and returns the messages to commit.
func (t *offsetTracker) ready() []kafka.Message {
	t.mu.Lock()
	defer t.mu.Unlock()
	for p, list := range t.pending {
		i := 0
		for i < len(list) && list[i].done {
			m := list[i].msg
			if c, ok := t.committed[p]; !ok || m.Offset > c {
				if cur, ok := t.readyMsg[p]; !ok || m.Offset > cur.Offset {
					t.readyMsg[p] = m
				}
			}
			i++
		}
		t.pending[p] = list[i:]
	}
	out := make([]kafka.Message, 0, len(t.readyMsg))
	for p, m := range t.readyMsg {
		out = append(out, m)
		delete(t.readyMsg, p)
	}
	return out
}

// requeue restores commit candidates after a failed checkpoint/commit.
func (t *offsetTracker) requeue(msgs []kafka.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range msgs {
		if cur, ok := t.readyMsg[m.Partition]; !ok || m.Offset > cur.Offset {
			t.readyMsg[m.Partition] = m
		}
	}
}

func (t *offsetTracker) markCommitted(msgs []kafka.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range msgs {
		if m.Offset > t.committed[m.Partition] {
			t.committed[m.Partition] = m.Offset
		}
	}
}
