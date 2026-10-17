package scheduler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
)

type subRecordingPubSub struct {
	active map[string]bool
	subs   int
}

func (r *subRecordingPubSub) Pub(context.Context, msgqueue.Topic, *msgqueue.Message) error {
	return nil
}

func (r *subRecordingPubSub) Sub(topic msgqueue.Topic, _ msgqueue.MsgHandler) (func() error, error) {
	r.subs++
	r.active[topic.Name()] = true

	return func() error {
		delete(r.active, topic.Name())
		return nil
	}, nil
}

func TestPartitionSubscriptionFollowsPartitionId(t *testing.T) {
	ps := &subRecordingPubSub{active: map[string]bool{}}
	sub := newPartitionSubscription(ps, func(*msgqueue.Message) error { return nil })

	oldTopic := msgqueue.SchedulerPartitionTopic("old").Name()
	newTopic := msgqueue.SchedulerPartitionTopic("new").Name()

	require.NoError(t, sub.sync("old"))
	require.NoError(t, sub.sync("old"))
	assert.Equal(t, 1, ps.subs, "unchanged partition id must not resubscribe")
	assert.True(t, ps.active[oldTopic])

	require.NoError(t, sub.sync("new"))
	assert.True(t, ps.active[newTopic])
	assert.False(t, ps.active[oldTopic], "old subscription must be dropped")

	require.NoError(t, sub.close())
	assert.Empty(t, ps.active)

	require.NoError(t, sub.sync("another"))
	assert.Empty(t, ps.active, "no subscribing after close")
}

func (r *subRecordingPubSub) IsReady() bool { return true }
