package scheduler

import (
	"sync"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
)

// partitionSubscription keeps a pubsub subscription pointed at the topic of the current
// scheduler partition. The partition id changes when the partition is re-created, and
// messages published to the new topic are dropped unless something is subscribed to it.
type partitionSubscription struct {
	pubsub  msgqueue.PubSub
	handler msgqueue.MsgHandler

	mu      sync.Mutex
	id      string
	cleanup func() error
	closed  bool
}

func newPartitionSubscription(pubsub msgqueue.PubSub, handler msgqueue.MsgHandler) *partitionSubscription {
	return &partitionSubscription{pubsub: pubsub, handler: handler}
}

// sync subscribes to the topic for partitionId and drops the previous subscription, if any.
// The new subscription is established first so no window exists with no subscriber.
func (p *partitionSubscription) sync(partitionId string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed || (p.cleanup != nil && p.id == partitionId) {
		return nil
	}

	cleanup, err := p.pubsub.Sub(msgqueue.SchedulerPartitionTopic(partitionId), p.handler)

	if err != nil {
		return err
	}

	old := p.cleanup
	p.id = partitionId
	p.cleanup = cleanup

	if old != nil {
		return old()
	}

	return nil
}

func (p *partitionSubscription) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true

	if p.cleanup == nil {
		return nil
	}

	cleanup := p.cleanup
	p.cleanup = nil

	return cleanup()
}
