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

package client

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gammazero/deque"
	"github.com/google/uuid"
)

// Subscriber represents a struct with a GCP Pub/Sub client,
// queues for messages, channels and a function to cancel the context.
type Subscriber struct {
	*pubSub

	lock       sync.Mutex
	msgDeque   *deque.Deque[*pubsub.Message]
	ackDeque   *deque.Deque[*pubsub.Message]
	errorCh    chan error
	canceledCh chan struct{}
	ctxCancel  context.CancelFunc
}

// NewSubscriber initializes a new subscriber client and starts receiving a messages to the queue.
func NewSubscriber(ctx context.Context, cfg config.Source) (*Subscriber, error) {
	ps, err := newClient(ctx, cfg.General)
	if err != nil {
		return nil, fmt.Errorf("new pubsub client: %w", err)
	}

	cctx, cancel := context.WithCancel(ctx)

	sub := &Subscriber{
		pubSub:     ps,
		msgDeque:   deque.New[*pubsub.Message](),
		ackDeque:   deque.New[*pubsub.Message](),
		errorCh:    make(chan error),
		canceledCh: make(chan struct{}),
		ctxCancel:  cancel,
	}

	go func() {
		s := sub.pubSub.client.Subscription(cfg.SubscriptionID)

		// set MaxExtension less than 0, to do not extend the ack deadline for each message
		s.ReceiveSettings.MaxExtension = -1

		if err = s.Receive(cctx,
			func(_ context.Context, m *pubsub.Message) {
				sub.lock.Lock()
				defer sub.lock.Unlock()

				sub.msgDeque.PushBack(m)
			},
		); err != nil {
			sub.errorCh <- fmt.Errorf("subscription receive: %w", err)
		}

		sub.canceledCh <- struct{}{}
	}()

	return sub, nil
}

// Next returns the next record or an error.
func (s *Subscriber) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case err := <-s.errorCh:
		return sdk.Record{}, err
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		s.lock.Lock()
		defer s.lock.Unlock()

		if s.msgDeque.Len() == 0 {
			return sdk.Record{}, sdk.ErrBackoffRetry
		}

		msg := s.msgDeque.PopFront()
		if msg == nil {
			return sdk.Record{}, sdk.ErrBackoffRetry
		}

		s.ackDeque.PushBack(msg)

		position, err := s.getPosition()
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
func (s *Subscriber) Ack(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		s.lock.Lock()
		defer s.lock.Unlock()

		if s.ackDeque.Len() == 0 {
			return nil
		}

		s.ackDeque.PopFront().Ack()

		return nil
	}
}

// Stop cancels the context to stop the GCP receiver,
// marks all unread messages the client did not receive them,
// waits the GCP receiver will stop and releases the GCP Pub/Sub client.
func (s *Subscriber) Stop() error {
	s.ctxCancel()

	s.lock.Lock()
	defer s.lock.Unlock()

	for s.msgDeque.Len() > 0 {
		s.msgDeque.PopFront().Nack()
	}

	for s.ackDeque.Len() > 0 {
		s.ackDeque.PopFront().Nack()
	}

	<-s.canceledCh

	return s.pubSub.close()
}

// getPosition returns the current iterator position.
func (*Subscriber) getPosition() (sdk.Position, error) {
	uuidBytes, err := uuid.New().MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshal uuid: %w", err)
	}

	return uuidBytes, nil
}
