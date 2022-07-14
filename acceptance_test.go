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

package gcppubsub

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"go.uber.org/goleak"
	"google.golang.org/api/option"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

// GenerateRecord generates a new record.
func (d driver) GenerateRecord(_ *testing.T) sdk.Record {
	return sdk.Record{
		Position:  nil,
		Metadata:  nil,
		CreatedAt: time.Now(),
		Key:       nil,
		Payload:   sdk.RawData(uuid.New().String()),
	}
}

func TestAcceptance(t *testing.T) {
	const (
		topicFmt = "acceptance-test-topic-%s"
		subFmt   = "acceptance-test-subscription-%s"
	)

	var (
		ctx     = context.Background()
		topicID = fmt.Sprintf(topicFmt, uuid.New().String())

		subscriptionIDs []string
	)

	privateKey := os.Getenv("GCP_PUBSUB_PRIVATE_KEY")
	if privateKey == "" {
		t.Skip("GCP_PUBSUB_PRIVATE_KEY env var must be set")
	}

	clientEmail := os.Getenv("GCP_PUBSUB_CLIENT_EMAIL")
	if clientEmail == "" {
		t.Skip("GCP_PUBSUB_CLIENT_EMAIL env var must be set")
	}

	projectID := os.Getenv("GCP_PUBSUB_PROJECT_ID")
	if projectID == "" {
		t.Skip("GCP_PUBSUB_PROJECT_ID env var must be set")
	}

	srcCfg := map[string]string{
		models.ConfigPrivateKey:  privateKey,
		models.ConfigClientEmail: clientEmail,
		models.ConfigProjectID:   projectID,
	}

	dstCfg := map[string]string{
		models.ConfigPrivateKey:  privateKey,
		models.ConfigClientEmail: clientEmail,
		models.ConfigProjectID:   projectID,
		models.ConfigTopicID:     topicID,
		models.ConfigBatchSize:   os.Getenv("GCP_PUBSUB_BATCH_SIZE"),
		models.ConfigBatchDelay:  os.Getenv("GCP_PUBSUB_BATCH_DELAY"),
	}

	credential, err := getCredential(srcCfg)
	if err != nil {
		t.Error(err)
	}

	if err = createTopic(ctx, projectID, topicID, credential); err != nil {
		t.Error(err)
	}

	defer func() {
		if err = cleanup(ctx, projectID, topicID, subscriptionIDs, credential); err != nil {
			t.Errorf("failed to cleanup resources: %s", err.Error())
		}
	}()

	sdk.AcceptanceTest(t, driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      srcCfg,
				DestinationConfig: dstCfg,
				BeforeTest: func(t *testing.T) {
					subscriptionID := fmt.Sprintf(subFmt, uuid.New().String())

					srcCfg[models.ConfigSubscriptionID] = subscriptionID

					err = createSubscription(ctx, projectID, topicID, subscriptionID, credential)
					if err != nil {
						t.Errorf("failed to create subscription: %s", err.Error())
					}

					subscriptionIDs = append(subscriptionIDs, subscriptionID)
				},
				GoleakOptions: []goleak.Option{
					// the go.opencensus.io module is used indirectly in the cloud.google.com/go/pubsub module
					goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
				},
				Skip: []string{
					// skip this test because it expects to receive data according to the FIFO principle.
					// GCP returns the data in random order
					"TestDestination_WriteAsync_Success",
				},
			},
		},
	})
}

func getCredential(src map[string]string) ([]byte, error) {
	return config.General{
		PrivateKey:  src[models.ConfigPrivateKey],
		ClientEmail: src[models.ConfigClientEmail],
		ProjectID:   src[models.ConfigProjectID],
	}.Marshal()
}

func createTopic(ctx context.Context, projectID, topicID string, credential []byte) error {
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	topic.Stop()

	return nil
}

func createSubscription(
	ctx context.Context, projectID, topicID, subscriptionID string, credential []byte,
) error {
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	topic := client.Topic(topicID)
	defer topic.Stop()

	if _, err = client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
		Topic: topic,
	}); err != nil {
		return fmt.Errorf("create subscription: %w", err)
	}

	return nil
}

func cleanup(
	ctx context.Context, projectID, topicID string, subscriptionIDs []string, credential []byte,
) error {
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	for i := range subscriptionIDs {
		if err = client.Subscription(subscriptionIDs[i]).Delete(ctx); err != nil {
			return fmt.Errorf("delete subscription %s: %w", subscriptionIDs[i], err)
		}
	}

	if err = client.Topic(topicID).Delete(ctx); err != nil {
		return fmt.Errorf("delete topic: %w", err)
	}

	return nil
}
