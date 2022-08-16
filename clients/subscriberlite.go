// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clients

import (
	"context"
	"fmt"
	"log"
	"sync"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/pscompat"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gammazero/deque"
	"google.golang.org/api/option"
)

const subscriptionPathFmt = "projects/%s/locations/%s/subscriptions/%s"

// SubscriberLite represents a struct with a GCP Pub/Sub Lite client,
// queues for messages, channels and a function to cancel the context.
type SubscriberLite struct {
	subscriber *pscompat.SubscriberClient

	lock       sync.Mutex
	msgDeque   *deque.Deque[*pubsub.Message]
	ackDeque   *deque.Deque[*pubsub.Message]
	errorCh    chan error
	canceledCh chan struct{}
	ctxCancel  context.CancelFunc
}

// NewSubscriberLite initializes a new subscriber client of GCP Pub/Sub Lite
// and starts receiving a messages to the queue.
func NewSubscriberLite(ctx context.Context, cfg config.Source) (*SubscriberLite, error) {
	credential, err := cfg.Marshal()
	if err != nil {
		return nil, err
	}

	subscriptionPath := fmt.Sprintf(subscriptionPathFmt, cfg.ProjectID, cfg.Location, cfg.SubscriptionID)

	subscriber, err := pscompat.NewSubscriberClientWithSettings(ctx, subscriptionPath, pscompat.ReceiveSettings{
		NackHandler: func(message *pubsub.Message) error {
			sdk.Logger(ctx).Info().Msgf("message wasn't ack: %q", message.ID)

			return nil
		},
	}, option.WithCredentialsJSON(credential))
	if err != nil {
		log.Fatalf("pscompat.NewSubscriberClientWithSettings error: %v", err)
	}

	cctx, cancel := context.WithCancel(ctx)

	sl := &SubscriberLite{
		subscriber: subscriber,
		msgDeque:   deque.New[*pubsub.Message](),
		ackDeque:   deque.New[*pubsub.Message](),
		errorCh:    make(chan error),
		canceledCh: make(chan struct{}),
		ctxCancel:  cancel,
	}

	go func() {
		if err = sl.subscriber.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
			sl.lock.Lock()
			defer sl.lock.Unlock()

			sl.msgDeque.PushBack(m)
		}); err != nil {
			close(sl.canceledCh)

			sl.errorCh <- fmt.Errorf("subscription receive: %w", err)
		}

		sl.canceledCh <- struct{}{}
	}()

	return sl, nil
}

// Next returns the next record or an error.
func (sl *SubscriberLite) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case err := <-sl.errorCh:
		return sdk.Record{}, err
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		sl.lock.Lock()
		defer sl.lock.Unlock()

		if sl.msgDeque.Len() == 0 {
			return sdk.Record{}, sdk.ErrBackoffRetry
		}

		msg := sl.msgDeque.PopFront()
		if msg == nil {
			return sdk.Record{}, sdk.ErrBackoffRetry
		}

		sl.ackDeque.PushBack(msg)

		position, err := getBinaryUUID()
		if err != nil {
			return sdk.Record{}, fmt.Errorf("get position: %w", err)
		}

		return sdk.Record{
			Position:  position,
			Metadata:  msg.Attributes,
			CreatedAt: msg.PublishTime,
			Payload:   sdk.RawData(msg.Data),
		}, nil
	}
}

// Ack indicates successful processing of a Message passed.
func (sl *SubscriberLite) Ack(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		sl.lock.Lock()
		defer sl.lock.Unlock()

		if sl.ackDeque.Len() == 0 {
			return nil
		}

		sl.ackDeque.PopFront().Ack()

		return nil
	}
}

// Stop cancels the context to stop the GCP receiver,
// logs all unread messages the client did not receive them,
// waits the GCP receiver will stop and releases the GCP Pub/Sub client.
func (sl *SubscriberLite) Stop() error {
	sl.ctxCancel()

	sl.lock.Lock()
	defer sl.lock.Unlock()

	for sl.msgDeque.Len() > 0 {
		sl.msgDeque.PopFront().Nack()
	}

	for sl.ackDeque.Len() > 0 {
		sl.ackDeque.PopFront().Nack()
	}

	<-sl.canceledCh

	return nil
}
