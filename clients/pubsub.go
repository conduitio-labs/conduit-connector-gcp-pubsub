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

	"cloud.google.com/go/pubsub"
	"github.com/conduitio/conduit-connector-gcp-pubsub/config"
	"google.golang.org/api/option"
)

// A PubSub represents a struct with a GCP Pub/Sub client,
// and channels for a message and an error.
type PubSub struct {
	Cli           *pubsub.Client
	MessagesCh    chan *pubsub.Message
	AckMessagesCh chan *pubsub.Message
	ErrorCh       chan error
}

// NewClient initializes a Pub/Sub client and starts receiving a messages to struct channels.
func NewClient(ctx context.Context, cfg config.Source) (*PubSub, error) {
	credential, err := cfg.General.Marshal()
	if err != nil {
		return nil, err
	}

	cli, err := pubsub.NewClient(ctx, cfg.ProjectID, option.WithCredentialsJSON(credential))
	if err != nil {
		return nil, fmt.Errorf("new pubsub client: %w", err)
	}

	pubSub := &PubSub{
		Cli:           cli,
		MessagesCh:    make(chan *pubsub.Message, pubsub.DefaultReceiveSettings.MaxOutstandingMessages),
		AckMessagesCh: make(chan *pubsub.Message, pubsub.DefaultReceiveSettings.MaxOutstandingMessages),
		ErrorCh:       make(chan error),
	}

	go func() {
		err = pubSub.Cli.Subscription(cfg.SubscriptionID).Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			pubSub.MessagesCh <- m
		})
		if err != nil {
			pubSub.ErrorCh <- fmt.Errorf("subscription receive: %w", err)
		}
	}()

	return pubSub, nil
}
