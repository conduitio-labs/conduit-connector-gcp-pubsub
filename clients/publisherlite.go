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
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/api/option"
)

const topicPathFmt = "projects/%s/locations/%s/topics/%s"

// A PublisherLite represents a publisher struct with a GCP Pub/Sub Lite client.
type PublisherLite struct {
	publisher *pscompat.PublisherClient

	resultsCh chan *result
	ctxCancel context.CancelFunc
}

// NewPublisherLite initializes a new publisher client of GCP Pub/Sub Lite.
func NewPublisherLite(ctx context.Context, cfg config.Destination, errAckCh chan error) (*PublisherLite, error) {
	credential, err := cfg.Marshal()
	if err != nil {
		return nil, err
	}

	topicPath := fmt.Sprintf(topicPathFmt, cfg.ProjectID, cfg.Location, cfg.TopicID)

	publisher, err := pscompat.NewPublisherClientWithSettings(ctx, topicPath, pscompat.PublishSettings{
		DelayThreshold: cfg.BatchDelay,
		CountThreshold: cfg.BatchSize,
	}, option.WithCredentialsJSON(credential))
	if err != nil {
		return nil, fmt.Errorf("create publisher client: %w", err)
	}

	cctx, cancel := context.WithCancel(ctx)

	publisherLite := &PublisherLite{
		publisher: publisher,
		resultsCh: make(chan *result, cfg.BatchSize),
		ctxCancel: cancel,
	}

	go func() {
		for {
			select {
			case <-cctx.Done():
				return
			case res := <-publisherLite.resultsCh:
				// blocks until the Publish call completes
				_, err := res.publishResult.Get(ctx)

				err = res.ackFunc(err)
				if err != nil {
					errAckCh <- fmt.Errorf("failed to call ackFunc: %w", err)
				}
			}
		}
	}()

	return publisherLite, nil
}

// Publish publishes a record to the GCP Pub/Sub Lite topic.
func (pl *PublisherLite) Publish(ctx context.Context, record sdk.Record, ackFunc sdk.AckFunc) {
	res := pl.publisher.Publish(ctx, &pubsub.Message{
		Data:       record.Payload.Bytes(),
		Attributes: record.Metadata,
	})

	pl.resultsCh <- &result{
		publishResult: res,
		ackFunc:       ackFunc,
	}
}

// Stop cancels the context, stops remaining published messages,
// and releases the GCP Pub/Sub Lite client.
func (pl *PublisherLite) Stop() error {
	pl.ctxCancel()
	pl.publisher.Stop()

	return nil
}
