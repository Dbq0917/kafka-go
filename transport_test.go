package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/createtopics"
	meta "github.com/segmentio/kafka-go/protocol/metadata"
	produceAPI "github.com/segmentio/kafka-go/protocol/produce"
)

func TestIssue477(t *testing.T) {
	// This test verifies that a connection attempt with a minimal TLS
	// configuration does not panic.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	cg := connGroup{
		addr: l.Addr(),
		pool: &connPool{
			dial: defaultDialer.DialContext,
			tls:  &tls.Config{},
		},
	}

	if _, err := cg.connect(context.Background(), cg.addr); err != nil {
		// An error is expected here because we are not actually establishing
		// a TLS connection to a kafka broker.
		t.Log(err)
	} else {
		t.Error("no error was reported when attempting to establish a TLS connection to a non-TLS endpoint")
	}
}

func TestIssue672(t *testing.T) {
	// ensure the test times out if the bug is re-introduced
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// we'll simulate a situation with one good topic and one bad topic (bad configuration)
	const brokenTopicName = "bad-topic"
	const okTopicName = "good-topic"

	// make the connection pool think it's immediately ready to send
	ready := make(chan struct{})
	close(ready)

	// allow the system to wake as much as it wants
	wake := make(chan event)
	defer close(wake)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-wake:
				if e == nil {
					return
				}
				e.trigger()
			}
		}
	}()

	// handle requests by immediately resolving them with a create topics response,
	// the "bad topic" will have an error value
	requests := make(chan connRequest, 1)
	defer close(requests)
	go func() {
		request := <-requests
		request.res.resolve(&createtopics.Response{
			ThrottleTimeMs: 0,
			Topics: []createtopics.ResponseTopic{
				{
					Name:         brokenTopicName,
					ErrorCode:    int16(InvalidPartitionNumber),
					ErrorMessage: InvalidPartitionNumber.Description(),
				},
				{
					Name:              okTopicName,
					NumPartitions:     1,
					ReplicationFactor: 1,
				},
			},
		})
	}()

	pool := &connPool{
		ready: ready,
		wake:  wake,
		conns: map[int32]*connGroup{},
	}

	// configure the state so it can find the good topic, but not the one that fails to create
	pool.setState(connPoolState{
		layout: protocol.Cluster{
			Topics: map[string]protocol.Topic{
				okTopicName: {
					Name: okTopicName,
					Partitions: map[int32]protocol.Partition{
						0: {},
					},
				},
			},
		},
	})

	// trick the connection pool into thinking it has a valid connection to a broker
	pool.conns[0] = &connGroup{
		pool:   pool,
		broker: Broker{},
		idleConns: []*conn{
			{
				reqs: requests,
			},
		},
	}

	// perform the round trip:
	// - if the issue is presenting this will hang waiting for metadata to arrive that will
	//   never arrive, causing a deadline timeout.
	// - if the issue is fixed this will resolve almost instantaneously
	r, err := pool.roundTrip(ctx, &createtopics.Request{
		Topics: []createtopics.RequestTopic{
			{
				Name:              brokenTopicName,
				NumPartitions:     0,
				ReplicationFactor: 1,
			},
			{
				Name:              okTopicName,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
	})
	// detect if the issue is presenting using the context timeout (note that checking the err return value
	// isn't good enough as the original implementation didn't return the context cancellation error due to
	// being run in a defer)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("issue 672 is presenting! roundTrip should not have timed out")
	}

	// ancillary assertions as general house-keeping, not directly related to the issue:

	// we're not expecting any errors in this test
	if err != nil {
		t.Fatalf("unexpected error provoking connection pool roundTrip: %v", err)
	}

	// we expect a response containing the errors from the broker
	if r == nil {
		t.Fatal("expected a non-nil response")
	}

	// we expect to have the create topic response with created earlier
	_, ok := r.(*createtopics.Response)
	if !ok {
		t.Fatalf("expected a createtopics.Response but got %T", r)
	}
}

func TestIssue806(t *testing.T) {
	// ensure the test times out if the bug is re-introduced
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// simulate unknown topic want auto create with unknownTopicName,
	const unknownTopicName = "unknown-topic"
	const okTopicName = "good-topic"

	// make the connection pool think it's immediately ready to send
	ready := make(chan struct{})
	close(ready)

	// allow the system to wake as much as it wants
	wake := make(chan event)
	defer close(wake)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-wake:
				if e == nil {
					return
				}
				e.trigger()
			}
		}
	}()

	// handle requests by immediately resolving them with a create topics response,
	// the "unknown topic" will have err UNKNOWN_TOPIC_OR_PARTITION
	requests := make(chan connRequest, 1)
	defer close(requests)
	go func() {
		request := <-requests
		request.res.resolve(&meta.Response{
			Topics: []meta.ResponseTopic{
				{
					Name:      unknownTopicName,
					ErrorCode: int16(UnknownTopicOrPartition),
				},
				{
					Name: okTopicName,
					Partitions: []meta.ResponsePartition{
						{
							PartitionIndex: 0,
						},
					},
				},
			},
		})
	}()

	pool := &connPool{
		ready: ready,
		wake:  wake,
		conns: map[int32]*connGroup{},
	}

	// configure the state,
	//
	// set cached metadata only have good topic,
	// so it need to request metadata,
	// caused by unknown topic cannot find in cached metadata
	//
	// set layout only have good topic,
	// so it can find the good topic, but not the one that fails to create
	pool.setState(connPoolState{
		metadata: &meta.Response{
			Topics: []meta.ResponseTopic{
				{
					Name: okTopicName,
					Partitions: []meta.ResponsePartition{
						{
							PartitionIndex: 0,
						},
					},
				},
			},
		},
		layout: protocol.Cluster{
			Topics: map[string]protocol.Topic{
				okTopicName: {
					Name: okTopicName,
					Partitions: map[int32]protocol.Partition{
						0: {},
					},
				},
			},
		},
	})

	// trick the connection pool into thinking it has a valid connection to request metadata
	pool.ctrl = &connGroup{
		pool:   pool,
		broker: Broker{},
		idleConns: []*conn{
			{
				reqs: requests,
			},
		},
	}

	// perform the round trip:
	// - if the issue is presenting this will hang waiting for metadata to arrive that will
	//   never arrive, causing a deadline timeout.
	// - if the issue is fixed this will resolve almost instantaneously
	r, err := pool.roundTrip(ctx, &meta.Request{
		TopicNames:             []string{unknownTopicName},
		AllowAutoTopicCreation: true,
	})
	// detect if the issue is presenting using the context timeout (note that checking the err return value
	// isn't good enough as the original implementation didn't return the context cancellation error due to
	// being run in a defer)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("issue 806 is presenting! roundTrip should not have timed out")
	}

	// ancillary assertions as general house-keeping, not directly related to the issue:

	// we're not expecting any errors in this test
	if err != nil {
		t.Fatalf("unexpected error provoking connection pool roundTrip: %v", err)
	}

	// we expect a response containing the errors from the broker
	if r == nil {
		t.Fatal("expected a non-nil response")
	}

	// we expect to have the create topic response with created earlier
	_, ok := r.(*meta.Response)
	if !ok {
		t.Fatalf("expected a meta.Response but got %T", r)
	}
}

func TestTransportMetadataRefreshOnLeaderFailover(t *testing.T) {
	// This test simulates a partition leader failover: after the leader of
	// the test topic's partition moves from broker 1 to broker 2, the
	// transport keeps routing writes to the previous leader, which has
	// become unreachable and times out instead of returning a redirection.
	//
	// The transport must detect the topology change from the write failures
	// and refresh its cached metadata promptly, instead of waiting for the
	// metadata TTL to expire.
	t.Run("old leader times out", func(t *testing.T) {
		testLeaderFailover(t, func(req connRequest) {
			req.res.reject(context.DeadlineExceeded)
		})
	})

	// Same scenario, but the previous leader is still alive and rejects the
	// writes with a NOT_LEADER_FOR_PARTITION error.
	t.Run("old leader rejects the write", func(t *testing.T) {
		testLeaderFailover(t, func(req connRequest) {
			req.res.resolve(&produceAPI.Response{
				Topics: []produceAPI.ResponseTopic{{
					Topic: "test-topic",
					Partitions: []produceAPI.ResponsePartition{{
						Partition: 0,
						ErrorCode: int16(NotLeaderForPartition),
					}},
				}},
			})
		})
	})
}

func testLeaderFailover(t *testing.T, onWriteToOldLeader func(connRequest)) {
	t.Helper()

	const topic = "test-topic"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// currentLeader holds the id of the broker that is currently the leader
	// for the test topic's only partition. It is flipped from 1 to 2 to
	// simulate a leader failover.
	var currentLeader int32 = 1

	var metadataRequests int32
	var broker1Requests int32
	var broker2Requests int32

	metadataReqs := make(chan connRequest)
	broker1Reqs := make(chan connRequest)
	broker2Reqs := make(chan connRequest)

	pool := &connPool{
		// A metadata TTL that is way longer than the test run ensures that
		// recovering from the leader failover can only come from refreshes
		// triggered by the write failures, not from the periodic refresh.
		metadataTTL: time.Hour,
		idleTimeout: time.Minute,
		ready:       make(event),
		wake:        make(chan event),
		conns:       make(map[int32]*connGroup),
	}

	// Pre-seed the cluster layout with the two brokers so that metadata
	// updates never add or remove connections from the pool.
	pool.setState(connPoolState{
		layout: protocol.Cluster{
			Brokers: map[int32]protocol.Broker{
				1: {Host: "localhost", Port: 9001, ID: 1},
				2: {Host: "localhost", Port: 9002, ID: 2},
			},
		},
	})

	ctrlConn := &conn{reqs: metadataReqs}
	pool.ctrl = &connGroup{
		pool:      pool,
		idleConns: []*conn{ctrlConn},
	}

	broker1Conn := &conn{reqs: broker1Reqs}
	broker2Conn := &conn{reqs: broker2Reqs}
	pool.conns[1] = &connGroup{pool: pool, idleConns: []*conn{broker1Conn}}
	pool.conns[2] = &connGroup{pool: pool, idleConns: []*conn{broker2Conn}}

	// Serve metadata requests from the fake control connection, reporting
	// the current value of currentLeader as the partition leader.
	go func() {
		for req := range metadataReqs {
			atomic.AddInt32(&metadataRequests, 1)
			req.res.resolve(&meta.Response{
				Brokers: []meta.ResponseBroker{
					{NodeID: 1, Host: "localhost", Port: 9001},
					{NodeID: 2, Host: "localhost", Port: 9002},
				},
				Topics: []meta.ResponseTopic{{
					Name: topic,
					Partitions: []meta.ResponsePartition{{
						PartitionIndex: 0,
						LeaderID:       atomic.LoadInt32(&currentLeader),
						ReplicaNodes:   []int32{1, 2},
						IsrNodes:       []int32{1, 2},
					}},
				}},
			})
			pool.ctrl.releaseConn(ctrlConn)
		}
	}()

	// Writes to broker 1 succeed until the failover, then behave according
	// to onWriteToOldLeader (timeout or not-leader error).
	go func() {
		for req := range broker1Reqs {
			atomic.AddInt32(&broker1Requests, 1)
			if atomic.LoadInt32(&currentLeader) == 1 {
				req.res.resolve(&produceAPI.Response{})
			} else {
				onWriteToOldLeader(req)
			}
			pool.conns[1].releaseConn(broker1Conn)
		}
	}()

	// Writes to broker 2 always succeed.
	go func() {
		for req := range broker2Reqs {
			atomic.AddInt32(&broker2Requests, 1)
			req.res.resolve(&produceAPI.Response{})
			pool.conns[2].releaseConn(broker2Conn)
		}
	}()

	go pool.discover(ctx, pool.wake)

	// Wait for the first metadata update to mark the pool as ready.
	<-pool.ready

	writeOnce := func() error {
		r, err := pool.roundTrip(ctx, &produceAPI.Request{
			Acks: 1,
			Topics: []produceAPI.RequestTopic{{
				Topic:      topic,
				Partitions: []produceAPI.RequestPartition{{Partition: 0}},
			}},
		})
		if err != nil {
			return err
		}
		res := r.(*produceAPI.Response)
		for i := range res.Topics {
			for j := range res.Topics[i].Partitions {
				if errCode := res.Topics[i].Partitions[j].ErrorCode; errCode != 0 {
					return Error(errCode)
				}
			}
		}
		return nil
	}

	// Normal writes must succeed and must not trigger metadata refreshes.
	for i := 0; i < 10; i++ {
		if err := writeOnce(); err != nil {
			t.Fatalf("write %d before failover: %v", i, err)
		}
	}
	if n := atomic.LoadInt32(&metadataRequests); n != 1 {
		t.Fatalf("metadata refreshed %d times during healthy writes, want 1", n)
	}
	if n := atomic.LoadInt32(&broker1Requests); n != 10 {
		t.Fatalf("writes routed to broker 1: got %d, want 10", n)
	}

	// Switch the partition leader from broker 1 to broker 2.
	atomic.StoreInt32(&currentLeader, 2)

	// Keep writing until the transport routes a write to the new leader,
	// counting the writes that fail while the metadata is stale.
	failedWrites := 0
	for {
		if err := writeOnce(); err == nil {
			break
		}
		failedWrites++
		select {
		case <-ctx.Done():
			t.Fatalf("writes did not recover after the leader failover (%d failed writes)", failedWrites)
		case <-time.After(time.Millisecond):
		}
	}

	t.Logf("writes recovered after %d failed writes", failedWrites)

	if n := atomic.LoadInt32(&broker2Requests); n != 1 {
		t.Errorf("writes routed to broker 2 after recovery: got %d, want 1", n)
	}

	// The metadata TTL being one hour, recovering can only have happened
	// because a write failure triggered a metadata refresh. Both the number
	// of failed writes and the number of metadata refreshes must be small:
	// the transport detects the topology change quickly, without refreshing
	// the metadata on every write.
	if failedWrites > 10 {
		t.Errorf("too many failed writes after the leader failover: %d", failedWrites)
	}
	if n := atomic.LoadInt32(&metadataRequests); n > 5 {
		t.Errorf("too many metadata refreshes after the leader failover: %d", n)
	}
}
