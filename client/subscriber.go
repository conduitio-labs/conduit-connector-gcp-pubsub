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
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// A Subscriber represents a subscriber struct with a GCP Pub/Sub client.
type Subscriber struct {
	*pubSub

	messagesCh    chan *pubsub.Message
	ackMessagesCh chan *pubsub.Message
	errorCh       chan error
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
		messagesCh:    make(chan *pubsub.Message, pubsub.DefaultReceiveSettings.MaxOutstandingMessages),
		ackMessagesCh: make(chan *pubsub.Message, pubsub.DefaultReceiveSettings.MaxOutstandingMessages),
		errorCh:       make(chan error),
		canceledCh:    make(chan struct{}),
		ctxCancel:     cancel,
	}

	go func() {
		err = subscriber.pubSub.client.Subscription(cfg.SubscriptionID).Receive(cctx,
			func(ctx context.Context, m *pubsub.Message) {
				subscriber.messagesCh <- m
			},
		)
		if err != nil {
			subscriber.errorCh <- fmt.Errorf("subscription receive: %w", err)
		}

		close(subscriber.messagesCh)

		subscriber.canceledCh <- struct{}{}
	}()

	return subscriber, nil
}

// Next receives and returns the next record or an error.
func (s *Subscriber) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case msg := <-s.messagesCh:
		s.ackMessagesCh <- msg

		return sdk.Record{
			Position:  sdk.Position(msg.ID),
			Metadata:  msg.Attributes,
			CreatedAt: msg.PublishTime,
			Payload:   sdk.RawData(msg.Data),
		}, nil
	case err := <-s.errorCh:
		return sdk.Record{}, err
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		return sdk.Record{}, sdk.ErrBackoffRetry
	}
}

// Ack indicates successful processing of a Message passed.
func (s *Subscriber) Ack(ctx context.Context) (string, error) {
	select {
	case msg := <-s.ackMessagesCh:
		msg.Ack()

		return msg.ID, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Stop cancels the context to stop the GCP receiver,
// marks all unread messages from the channel the client did not receive them,
// waits the GCP receiver will stop and releases the GCP Pub/Sub client.
func (s *Subscriber) Stop() error {
	s.ctxCancel()

	for msg := range s.messagesCh {
		msg.Nack()
	}

	<-s.canceledCh

	return s.pubSub.close()
}
