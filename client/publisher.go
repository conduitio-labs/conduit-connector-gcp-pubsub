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

// A PublisherInterface represents a publisher interface.
type PublisherInterface interface {
	Publish(context.Context, sdk.Record, sdk.AckFunc)
	Stop() error
}

// A Publisher represents a publisher struct with a GCP Pub/Sub client.
type Publisher struct {
	*pubSub

	topic     *pubsub.Topic
	resultsCh chan *result
	ctxCancel context.CancelFunc
}

type result struct {
	publishResult *pubsub.PublishResult
	ackFunc       sdk.AckFunc
}

// NewPublisher initializes a new publisher client.
func NewPublisher(ctx context.Context, cfg config.Destination) (PublisherInterface, error) {
	ps, err := newClient(ctx, cfg.General)
	if err != nil {
		return nil, fmt.Errorf("new pubsub client: %w", err)
	}

	cctx, cancel := context.WithCancel(ctx)

	publisher := &Publisher{
		pubSub:    ps,
		topic:     ps.client.Topic(cfg.TopicID),
		resultsCh: make(chan *result, cfg.BatchSize),
		ctxCancel: cancel,
	}

	publisher.topic.PublishSettings.CountThreshold = cfg.BatchSize
	publisher.topic.PublishSettings.DelayThreshold = cfg.BatchDelay

	go func() {
		for {
			select {
			case <-cctx.Done():
				return
			case msg := <-publisher.resultsCh:
				<-msg.publishResult.Ready()

				_, err := msg.publishResult.Get(ctx)
				err = msg.ackFunc(err)
				if err != nil {
					sdk.Logger(ctx).Err(err).Msg("failed to call ackFunc")
				}
			}
		}
	}()

	return publisher, nil
}

// Publish publishes a record to the GCP Pub/Sub topic.
func (p *Publisher) Publish(ctx context.Context, record sdk.Record, ackFunc sdk.AckFunc) {
	res := p.topic.Publish(ctx, &pubsub.Message{
		Data:       record.Payload.Bytes(),
		Attributes: record.Metadata,
	})

	p.resultsCh <- &result{
		publishResult: res,
		ackFunc:       ackFunc,
	}
}

// Stop cancels the context, stops remaining published messages,
// and releases the GCP Pub/Sub client.
func (p *Publisher) Stop() error {
	p.ctxCancel()
	p.topic.Stop()

	return p.client.Close()
}
