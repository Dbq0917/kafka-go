package kafka

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
	produceAPI "github.com/segmentio/kafka-go/protocol/produce"
)

// mockTransport is a RoundTripper that serves metadata requests locally and
// counts produce requests, allowing batching behavior to be tested without a
// running kafka broker.
type mockTransport struct {
	partitions int32

	produceRequests int64
	produceMessages int64
	mu              sync.Mutex
	produceLatency  time.Duration
}

func (t *mockTransport) RoundTrip(_ context.Context, _ net.Addr, req Request) (Response, error) {
	switch r := req.(type) {
	case *metadataAPI.Request:
		partitions := make([]metadataAPI.ResponsePartition, t.partitions)
		for i := range partitions {
			partitions[i] = metadataAPI.ResponsePartition{
				ErrorCode:      0,
				PartitionIndex: int32(i),
				LeaderID:       1,
			}
		}
		topics := make([]metadataAPI.ResponseTopic, 0, len(r.TopicNames))
		for _, name := range r.TopicNames {
			topics = append(topics, metadataAPI.ResponseTopic{
				Name:       name,
				Partitions: partitions,
			})
		}
		return &metadataAPI.Response{Topics: topics}, nil

	case *produceAPI.Request:
		if latency := t.latency(); latency > 0 {
			time.Sleep(latency)
		}

		var count int
		topics := make([]produceAPI.ResponseTopic, 0, len(r.Topics))
		for _, topic := range r.Topics {
			partitions := make([]produceAPI.ResponsePartition, 0, len(topic.Partitions))
			for _, partition := range topic.Partitions {
				if records, ok := partition.RecordSet.Records.(*writerRecords); ok {
					count += len(records.msgs)
				}
				partitions = append(partitions, produceAPI.ResponsePartition{
					Partition: partition.Partition,
				})
			}
			topics = append(topics, produceAPI.ResponseTopic{
				Topic:      topic.Topic,
				Partitions: partitions,
			})
		}
		atomic.AddInt64(&t.produceRequests, 1)
		atomic.AddInt64(&t.produceMessages, int64(count))
		return &produceAPI.Response{Topics: topics}, nil
	}

	return nil, nil
}

func (t *mockTransport) latency() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.produceLatency
}

func (t *mockTransport) produceRequestCount() int64 {
	return atomic.LoadInt64(&t.produceRequests)
}

func (t *mockTransport) produceMessageCount() int64 {
	return atomic.LoadInt64(&t.produceMessages)
}

// newBatchingTestWriter builds a writer backed by mockTransport with a single
// partition so every produce request maps to one broker request.
func newBatchingTestWriter(t *testing.T, transport *mockTransport, config WriterConfig) *Writer {
	t.Helper()
	w := newTestWriter(config)
	w.Transport = transport
	return w
}

// TestWriterBatchingHighTraffic verifies that under a sustained load the
// linger timer keeps extending and batches are flushed by size instead of
// being cut into many small requests, keeping the number of produce requests
// close to messages/BatchSize.
func TestWriterBatchingHighTraffic(t *testing.T) {
	transport := &mockTransport{partitions: 1}

	const (
		messageCount = 2000
		batchSize    = 500
	)

	writer := newBatchingTestWriter(t, transport, WriterConfig{
		Brokers:         []string{"mock:9092"},
		Topic:           "batching-high-traffic",
		BatchSize:       batchSize,
		BatchBytes:      4 * 1024 * 1024,
		BatchTimeout:    time.Second,
		LingerTime:      20 * time.Millisecond,
		MaxBatchTimeout: time.Second,
	})
	defer writer.Close()

	msgs := make([]Message, messageCount)
	for i := range msgs {
		msgs[i] = Message{Value: make([]byte, 1024)}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	if err := writer.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("WriteMessages: %v", err)
	}
	elapsed := time.Since(start)

	if got := transport.produceMessageCount(); got != messageCount {
		t.Fatalf("produced %d messages, want %d", got, messageCount)
	}

	requests := transport.produceRequestCount()
	wantRequests := int64(messageCount / batchSize)
	if requests != wantRequests {
		t.Fatalf("high traffic produced %d requests, want %d (messages=%d, batchSize=%d)",
			requests, wantRequests, messageCount, batchSize)
	}

	if elapsed >= time.Second {
		t.Fatalf("high traffic batching took %s, batches should flush by size without waiting for BatchTimeout", elapsed)
	}

	t.Logf("high traffic: %d messages -> %d produce requests in %s", messageCount, requests, elapsed)
}

// TestWriterBatchingLowTraffic verifies that an isolated message is flushed
// after LingerTime rather than waiting for BatchTimeout, so low-traffic
// producers do not pay the full batching timeout in latency.
func TestWriterBatchingLowTraffic(t *testing.T) {
	transport := &mockTransport{partitions: 1}

	writer := newBatchingTestWriter(t, transport, WriterConfig{
		Brokers:         []string{"mock:9092"},
		Topic:           "batching-low-traffic",
		BatchSize:       1000,
		BatchBytes:      4 * 1024 * 1024,
		BatchTimeout:    time.Second,
		LingerTime:      20 * time.Millisecond,
		MaxBatchTimeout: time.Second,
	})
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	if err := writer.WriteMessages(ctx, Message{Value: make([]byte, 1024)}); err != nil {
		t.Fatalf("WriteMessages: %v", err)
	}
	elapsed := time.Since(start)

	if got := transport.produceRequestCount(); got != 1 {
		t.Fatalf("low traffic produced %d requests, want 1", got)
	}

	if elapsed >= time.Second {
		t.Fatalf("isolated message took %s, should flush after LingerTime (20ms), not BatchTimeout (1s)", elapsed)
	}

	t.Logf("low traffic: 1 message -> 1 produce request in %s", elapsed)
}

// TestWriterBatchingRequestCountComparison runs a high-flow and a low-flow
// scenario with the same volume and logs their produce request counts,
// demonstrating that high flow is bounded by BatchSize while low flow flushes
// partial batches promptly through LingerTime rather than BatchTimeout.
func TestWriterBatchingRequestCountComparison(t *testing.T) {
	const (
		messageCount = 100
		batchSize    = 100
	)

	scenarios := []struct {
		name       string
		linger     time.Duration
		timeout    time.Duration
		interval   time.Duration
		wantMaxReq int64
		wantMinAvg float64
	}{
		{
			// Fast producers fill the batch immediately; the linger window
			// extends while messages keep arriving so the request count is
			// bounded by BatchSize instead of being multiplied.
			name:       "high flow with linger",
			linger:     20 * time.Millisecond,
			timeout:    time.Second,
			interval:   0,
			wantMaxReq: 1,
			wantMinAvg: batchSize,
		},
		{
			// Messages arrive more slowly than the linger window, so each
			// message flushes its own partial batch promptly instead of being
			// held for the full BatchTimeout. The request count stays high but
			// latency stays low (bounded by LingerTime, not BatchTimeout).
			name:       "low flow with linger",
			linger:     20 * time.Millisecond,
			timeout:    time.Second,
			interval:   30 * time.Millisecond,
			wantMaxReq: messageCount,
			wantMinAvg: 1,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			transport := &mockTransport{partitions: 1}
			writer := &Writer{
				Addr:            TCP("mock:9092"),
				Topic:           "batching-comparison",
				BatchSize:       batchSize,
				BatchBytes:      4 * 1024 * 1024,
				BatchTimeout:    scenario.timeout,
				LingerTime:      scenario.linger,
				MaxBatchTimeout: scenario.timeout,
				Async:           true,
				Transport:       transport,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			totalStart := time.Now()
			for i := 0; i < messageCount; i++ {
				if err := writer.WriteMessages(ctx, Message{Value: make([]byte, 1024)}); err != nil {
					t.Fatalf("WriteMessages #%d: %v", i, err)
				}
				if scenario.interval > 0 {
					time.Sleep(scenario.interval)
				}
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			requests := transport.produceRequestCount()
			messages := transport.produceMessageCount()
			if messages != messageCount {
				t.Fatalf("produced %d messages, want %d", messages, messageCount)
			}
			if requests > scenario.wantMaxReq {
				t.Fatalf("produced %d requests, want at most %d", requests, scenario.wantMaxReq)
			}
			avgBatch := float64(messageCount) / float64(requests)
			if avgBatch < scenario.wantMinAvg {
				t.Fatalf("average batch size %.1f, want at least %.1f", avgBatch, scenario.wantMinAvg)
			}

			t.Logf("%s: %d messages -> %d produce requests in %s (avg batch %.1f)",
				scenario.name, messageCount, requests, time.Since(totalStart), avgBatch)
		})
	}
}

// TestWriterBatchingDeadlineCap verifies that MaxBatchTimeout flushes a batch
// even when messages keep arriving before each linger window elapses.
func TestWriterBatchingDeadlineCap(t *testing.T) {
	transport := &mockTransport{partitions: 1}

	const maxBatchTimeout = 100 * time.Millisecond
	writer := newBatchingTestWriter(t, transport, WriterConfig{
		Brokers:         []string{"mock:9092"},
		Topic:           "batching-deadline",
		BatchSize:       100000,
		BatchBytes:      64 * 1024 * 1024,
		BatchTimeout:    time.Second,
		LingerTime:      50 * time.Millisecond,
		MaxBatchTimeout: maxBatchTimeout,
	})
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = writer.WriteMessages(ctx, Message{Value: make([]byte, 128)})
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	time.Sleep(350 * time.Millisecond)
	close(stop)
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	requests := transport.produceRequestCount()
	if requests < 2 {
		t.Fatalf("expected MaxBatchTimeout to flush partial batches at least twice, got %d requests", requests)
	}
	t.Logf("deadline cap: %d produce requests over ~350ms with 100ms max batch timeout", requests)
}

// TestWriterBatchingDefaultLinger verifies that a writer configured with only
// the historical fields still flushes an isolated message promptly (via the
// default LingerTime) instead of waiting for the default BatchTimeout of 1s.
func TestWriterBatchingDefaultLinger(t *testing.T) {
	transport := &mockTransport{partitions: 1}

	writer := &Writer{
		Addr:      TCP("mock:9092"),
		Topic:     "batching-default-linger",
		Transport: transport,
	}
	defer writer.Close()

	if got := writer.lingerTime(); got != 20*time.Millisecond {
		t.Fatalf("default linger = %s, want 20ms", got)
	}
	if got := writer.maxBatchTimeout(); got != writer.batchTimeout() {
		t.Fatalf("default max batch timeout = %s, want %s", got, writer.batchTimeout())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	if err := writer.WriteMessages(ctx, Message{Value: make([]byte, 1024)}); err != nil {
		t.Fatalf("WriteMessages: %v", err)
	}

	if elapsed := time.Since(start); elapsed >= writer.batchTimeout() {
		t.Fatalf("isolated message took %s, default linger should flush before %s", elapsed, writer.batchTimeout())
	}
	if got := transport.produceRequestCount(); got != 1 {
		t.Fatalf("produced %d requests, want 1", got)
	}
}

// TestWriterBatchingSendingThreadNotBlocked verifies that timer management
// never blocks the calling goroutine: with a very long batch timeout, a
// WriteMessages call that fills no batch returns immediately, and the flush
// happens asynchronously.
func TestWriterBatchingSendingThreadNotBlocked(t *testing.T) {
	transport := &mockTransport{partitions: 1}

	writer := &Writer{
		Addr:         TCP("mock:9092"),
		Topic:        "batching-non-blocking",
		BatchSize:    1_000_000,
		BatchBytes:   64 * 1024 * 1024,
		BatchTimeout: time.Hour,
		LingerTime:   50 * time.Millisecond,
		Transport:    transport,
		Async:        true,
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	if err := writer.WriteMessages(ctx, Message{Value: make([]byte, 1024)}); err != nil {
		t.Fatalf("WriteMessages: %v", err)
	}
	if blockedFor := time.Since(start); blockedFor > 100*time.Millisecond {
		t.Fatalf("WriteMessages blocked for %s, timer logic must not block the sending thread", blockedFor)
	}

	if got := transport.produceRequestCount(); got != 0 {
		t.Fatalf("produce request count = %d, batch should still be pending", got)
	}
}

// TestWriterBatchingRepeatedClose exercises timer lifecycle across many
// writer open/close cycles to ensure no timer callback or goroutine is leaked.
func TestWriterBatchingRepeatedClose(t *testing.T) {
	for cycle := 0; cycle < 25; cycle++ {
		transport := &mockTransport{partitions: 1}
		writer := &Writer{
			Addr:            TCP("mock:9092"),
			Topic:           "batching-repeated-close",
			BatchSize:       1000,
			BatchTimeout:    time.Second,
			LingerTime:      5 * time.Millisecond,
			MaxBatchTimeout: 50 * time.Millisecond,
			Transport:       transport,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		for i := 0; i < 10; i++ {
			if err := writer.WriteMessages(ctx, Message{Value: make([]byte, 64)}); err != nil {
				t.Fatalf("cycle %d WriteMessages #%d: %v", cycle, i, err)
			}
		}
		cancel()

		done := make(chan struct{})
		go func() {
			_ = writer.Close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("cycle %d: Close did not return, timer callback likely leaked", cycle)
		}

		if got := transport.produceMessageCount(); got != 10 {
			t.Fatalf("cycle %d: produced %d messages, want 10", cycle, got)
		}
	}
}
