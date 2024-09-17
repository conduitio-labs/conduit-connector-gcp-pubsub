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
	"cloud.google.com/go/pubsublite/pscompat"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"google.golang.org/api/option"
)

// A PublisherLite represents a publisher struct with a GCP Pub/Sub Lite client.
type PublisherLite struct {
	publisher *pscompat.PublisherClient
}

// NewPublisherLite initializes a new publisher client of GCP Pub/Sub Lite.
func NewPublisherLite(ctx context.Context, cfg config.Destination) (*PublisherLite, error) {
	const topicPathFmt = "projects/%s/locations/%s/topics/%s"

	credential, err := cfg.Marshal()
	if err != nil {
		return nil, err
	}

	publisher, err := pscompat.NewPublisherClient(ctx,
		fmt.Sprintf(topicPathFmt, cfg.ProjectID, cfg.Location, cfg.TopicID), option.WithCredentialsJSON(credential))
	if err != nil {
		return nil, fmt.Errorf("create lite publisher client: %w", err)
	}

	return &PublisherLite{
		publisher: publisher,
	}, nil
}

// Publish publishes a record to the GCP Pub/Sub Lite topic.
func (pl *PublisherLite) Publish(ctx context.Context, record opencdc.Record) error {
	_, err := pl.publisher.Publish(ctx, &pubsub.Message{
		Data:       record.Payload.After.Bytes(),
		Attributes: record.Metadata,
	}).Get(ctx)
	if err != nil {
		return fmt.Errorf("publish message: %w", err)
	}

	return nil
}

// Stop sends all remaining published messages and closes publish streams.
func (pl *PublisherLite) Stop() error {
	pl.publisher.Stop()

	return nil
}
