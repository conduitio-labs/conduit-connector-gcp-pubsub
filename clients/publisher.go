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
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio/conduit-commons/opencdc"
)

// A Publisher represents a publisher struct with a GCP Pub/Sub client.
type Publisher struct {
	*pubSub

	topic *pubsub.Topic
}

// NewPublisher initializes a new publisher client.
func NewPublisher(ctx context.Context, cfg config.Destination) (*Publisher, error) {
	ps, err := newClient(ctx, cfg.General)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		pubSub: ps,
		topic:  ps.client.Topic(cfg.TopicID),
	}, nil
}

// Publish publishes records to the GCP Pub/Sub topic.
func (p *Publisher) Publish(ctx context.Context, record opencdc.Record) error {
	_, err := p.topic.Publish(ctx, &pubsub.Message{
		Data:       record.Payload.After.Bytes(),
		Attributes: record.Metadata,
	}).Get(ctx)
	if err != nil {
		return fmt.Errorf("publish message: %w", err)
	}

	return nil
}

// Stop sends all remaining published messages and stop goroutines created for handling publishing,
// and releases any resources held by the client, such as memory and goroutines.
func (p *Publisher) Stop() error {
	p.topic.Stop()

	return p.close()
}
