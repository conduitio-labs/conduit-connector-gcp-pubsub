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

// A Subscriber represents a struct with a GCP subscriber client.
type Subscriber struct {
	MessagesCh    chan *pubsub.Message
	AckMessagesCh chan *pubsub.Message
	ErrorCh       chan error
	cli           *pubsub.Client
	canceledCh    chan struct{}
	ctxCancel     context.CancelFunc
}

// NewSubscriber initializes a new subscriber client and starts receiving a messages to struct channels.
func NewSubscriber(ctx context.Context, cfg config.Source) (*Subscriber, error) {
	cli, err := newClient(ctx, cfg.General)
	if err != nil {
		return nil, fmt.Errorf("new pubsub client: %w", err)
	}

	cctx, cancel := context.WithCancel(ctx)

	pubSub := &Subscriber{
		cli:           cli,
		MessagesCh:    make(chan *pubsub.Message, pubsub.DefaultReceiveSettings.MaxOutstandingMessages),
		AckMessagesCh: make(chan *pubsub.Message, pubsub.DefaultReceiveSettings.MaxOutstandingMessages),
		ErrorCh:       make(chan error),
		canceledCh:    make(chan struct{}),
		ctxCancel:     cancel,
	}

	go func() {
		err = pubSub.cli.Subscription(cfg.SubscriptionID).Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
			pubSub.MessagesCh <- m
		})
		if err != nil {
			pubSub.ErrorCh <- fmt.Errorf("subscription receive: %w", err)
		}

		close(pubSub.MessagesCh)

		pubSub.canceledCh <- struct{}{}
	}()

	return pubSub, nil
}

// Close cancels the context to stop the GCP receiver,
// marks all unread messages from the channel the client did not receive them,
// waits the GCP receiver will stop and releases the GCP subscriber client.
func (ps *Subscriber) Close() error {
	if ps == nil {
		return nil
	}

	ps.ctxCancel()

	for msg := range ps.MessagesCh {
		msg.Nack()
	}

	<-ps.canceledCh

	return ps.cli.Close()
}
