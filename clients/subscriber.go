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
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gammazero/deque"
	"github.com/google/uuid"
	"go.uber.org/multierr"
)

// Subscriber represents a struct with a GCP Pub/Sub client,
// queues for messages, an error channel.
type Subscriber struct {
	*pubSub
	*subscriber
}

type subscriber struct {
	// mutex to lock subscriber during queue operation
	mu sync.Mutex

	// queue to store all messages until they are read
	msgDeque *deque.Deque[*pubsub.Message]
	// queue to acknowledge messages
	ackDeque *deque.Deque[*pubsub.Message]
	// error chanel for the receiver's error
	errorCh chan error
}

// NewSubscriber initializes a new subscriber client and starts receiving a messages to the queue.
func NewSubscriber(ctx context.Context, cfg config.Source) (*Subscriber, error) {
	ps, err := newClient(ctx, cfg.General)
	if err != nil {
		return nil, err
	}

	sub := &Subscriber{
		pubSub: ps,
		subscriber: &subscriber{
			msgDeque: deque.New[*pubsub.Message](),
			ackDeque: deque.New[*pubsub.Message](),
			errorCh:  make(chan error),
		},
	}

	go func() {
		err = sub.pubSub.client.Subscription(cfg.SubscriptionID).Receive(ctx,
			func(_ context.Context, m *pubsub.Message) {
				sub.mu.Lock()
				defer sub.mu.Unlock()

				sub.msgDeque.PushBack(m)
			},
		)
		if err != nil {
			sub.errorCh <- fmt.Errorf("subscription receive: %w", err)

			return
		}

		sub.errorCh <- nil
	}()

	return sub, nil
}

// Next returns the next record or an error.
func (s *subscriber) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case err := <-s.errorCh:
		return sdk.Record{}, err
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		s.mu.Lock()
		defer s.mu.Unlock()

		if s.msgDeque.Len() == 0 {
			return sdk.Record{}, sdk.ErrBackoffRetry
		}

		msg := s.msgDeque.PopFront()

		s.ackDeque.PushBack(msg)

		metadata := sdk.Metadata{}
		if len(msg.Attributes) > 0 {
			metadata = msg.Attributes
		}
		metadata.SetCreatedAt(msg.PublishTime)

		return sdk.Util.Source.NewRecordCreate(
			sdk.Position(uuid.NewString()),
			metadata,
			nil,
			sdk.RawData(msg.Data),
		), nil
	}
}

// Ack indicates successful processing of a Message passed.
func (s *subscriber) Ack(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		s.mu.Lock()
		defer s.mu.Unlock()

		s.ackDeque.PopFront().Ack()

		return nil
	}
}

// Stop calls stop method and releases the GCP Pub/Sub client.
func (s *Subscriber) Stop() error {
	return multierr.Append(s.stop(), s.pubSub.close())
}

// stop cancels the context to stop the GCP receiver,
// marks all unread messages the client did not receive them,
// and waits the GCP receiver will stop.
func (s *subscriber) stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.msgDeque.Len() > 0 {
		s.msgDeque.PopFront().Nack()
	}

	for s.ackDeque.Len() > 0 {
		s.ackDeque.PopFront().Nack()
	}

	return <-s.errorCh
}
