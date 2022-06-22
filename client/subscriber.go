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

	"cloud.google.com/go/pubsub"
	"github.com/conduitio/conduit-connector-gcp-pubsub/config"
)

// A Subscriber represents a subscriber struct with a GCP Pub/Sub client.
type Subscriber struct {
	*pubSub

	MessagesCh    chan *pubsub.Message
	AckMessagesCh chan *pubsub.Message
	ErrorCh       chan error
	canceledCh    chan struct{}
	ctxCancel     context.CancelFunc
}

// NewSubscriber initializes a new subscriber client and starts receiving a messages to struct channels.
func NewSubscriber(ctx context.Context, cfg config.Source) (*Subscriber, error) {
	ps, err := newClient(ctx, cfg.General)
	if err != nil {
		return nil, fmt.Errorf("new pubsub client: %w", err)
	}

	cctx, cancel := context.WithCancel(ctx)

	subscriber := &Subscriber{
		pubSub:        ps,
		MessagesCh:    make(chan *pubsub.Message, pubsub.DefaultReceiveSettings.MaxOutstandingMessages),
		AckMessagesCh: make(chan *pubsub.Message, pubsub.DefaultReceiveSettings.MaxOutstandingMessages),
		ErrorCh:       make(chan error),
		canceledCh:    make(chan struct{}),
		ctxCancel:     cancel,
	}

	go func() {
		err = subscriber.pubSub.client.Subscription(cfg.SubscriptionID).Receive(cctx,
			func(ctx context.Context, m *pubsub.Message) {
				subscriber.MessagesCh <- m
			},
		)
		if err != nil {
			subscriber.ErrorCh <- fmt.Errorf("subscription receive: %w", err)
		}

		close(subscriber.MessagesCh)

		subscriber.canceledCh <- struct{}{}
	}()

	return subscriber, nil
}

// Close cancels the context to stop the GCP receiver,
// marks all unread messages from the channel the client did not receive them,
// waits the GCP receiver will stop and releases the GCP subscriber client.
func (s *Subscriber) Close() error {
	if s == nil {
		return nil
	}

	s.ctxCancel()

	for msg := range s.MessagesCh {
		msg.Nack()
	}

	<-s.canceledCh

	return s.pubSub.close()
}
