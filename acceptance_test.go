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
	"cloud.google.com/go/pubsublite"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"go.uber.org/goleak"
	"google.golang.org/api/option"
)

const (
	topicFmt        = "acceptance-test-topic-%d"
	subscriptionFmt = "acceptance-test-subscription-%d"

	reservationPathFmt  = "projects/%s/locations/%s/reservations/reservation-%d"
	topicPathFmt        = "projects/%s/locations/%s/topics/%s"
	subscriptionPathFmt = "projects/%s/locations/%s/subscriptions/%s"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

// GenerateRecord generates a new record.
func (d driver) GenerateRecord(_ *testing.T, op sdk.Operation) sdk.Record {
	return sdk.Record{
		Position:  sdk.Position(uuid.NewString()),
		Operation: op,
		Metadata:  sdk.Metadata{uuid.NewString(): uuid.NewString()},
		Payload:   sdk.Change{After: sdk.RawData(uuid.NewString())},
	}
}

func TestAcceptance(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)

		subscriptionIDs []string

		reservation string
	)

	credential, err := getCredential(cfg)
	if err != nil {
		t.Error(err)
	}

	if cfg[models.ConfigLocation] != "" {
		reservation = fmt.Sprintf(reservationPathFmt,
			cfg[models.ConfigProjectID], cfg[models.ConfigLocation], time.Now().Unix())
	}

	if err = prepareResources(ctx, cfg, reservation, credential); err != nil {
		t.Error(err)
	}

	t.Cleanup(func() {
		if err = cleanupResources(ctx, cfg, reservation, subscriptionIDs, credential); err != nil {
			t.Errorf("failed to cleanup resources: %s", err.Error())
		}
	})

	sdk.AcceptanceTest(t, driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				BeforeTest: func(t *testing.T) {
					subscriptionID := fmt.Sprintf(subscriptionFmt, time.Now().Unix())

					cfg[models.ConfigSubscriptionID] = subscriptionID

					err = createSubscription(ctx, cfg, credential)
					if err != nil {
						t.Errorf("failed to create subscription: %s", err.Error())
					}

					subscriptionIDs = append(subscriptionIDs, subscriptionID)
				},
				GoleakOptions: []goleak.Option{
					// the go.opencensus.io module is used indirectly in the cloud.google.com/go/pubsub module
					goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
				},
			},
		},
	})
}

func prepareConfig(t *testing.T) map[string]string {
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

	return map[string]string{
		models.ConfigPrivateKey:  privateKey,
		models.ConfigClientEmail: clientEmail,
		models.ConfigProjectID:   projectID,
		models.ConfigTopicID:     fmt.Sprintf(topicFmt, time.Now().Unix()),
	}
}

func getCredential(src map[string]string) ([]byte, error) {
	return config.General{
		PrivateKey:  src[models.ConfigPrivateKey],
		ClientEmail: src[models.ConfigClientEmail],
		ProjectID:   src[models.ConfigProjectID],
	}.Marshal()
}

func prepareResources(ctx context.Context, cfg map[string]string, reservation string, credential []byte) error {
	if cfg[models.ConfigLocation] == "" {
		client, err := pubsub.NewClient(ctx, cfg[models.ConfigProjectID], option.WithCredentialsJSON(credential))
		if err != nil {
			return fmt.Errorf("new client: %w", err)
		}
		defer client.Close()

		topic, err := client.CreateTopic(ctx, cfg[models.ConfigTopicID])
		if err != nil {
			return fmt.Errorf("create topic: %w", err)
		}
		topic.Stop()

		return nil
	}

	const gib = 1 << 30

	admin, err := pubsublite.NewAdminClient(ctx, cfg[models.ConfigLocation], option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new admin: %w", err)
	}
	defer admin.Close()

	reservationConfig := pubsublite.ReservationConfig{
		Name:               reservation,
		ThroughputCapacity: 1,
	}

	_, err = admin.CreateReservation(ctx, reservationConfig)
	if err != nil {
		return fmt.Errorf("create reservation: %w", err)
	}

	topicPath := fmt.Sprintf(topicPathFmt,
		cfg[models.ConfigProjectID], cfg[models.ConfigLocation], cfg[models.ConfigTopicID])

	topicConfig := pubsublite.TopicConfig{
		Name:                       topicPath,
		PartitionCount:             1,        // Must be at least 1.
		PublishCapacityMiBPerSec:   4,        // Must be 4-16 MiB/s.
		SubscribeCapacityMiBPerSec: 4,        // Must be 4-32 MiB/s.
		PerPartitionBytes:          30 * gib, // Must be 30 GiB-10 TiB.
		ThroughputReservation:      reservation,
		// Retain messages indefinitely as long as there is available storage.
		RetentionDuration: pubsublite.InfiniteRetention,
	}

	_, err = admin.CreateTopic(ctx, topicConfig)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}

	return nil
}

func createSubscription(
	ctx context.Context, cfg map[string]string, credential []byte,
) error {
	if cfg[models.ConfigLocation] == "" {
		client, err := pubsub.NewClient(ctx, cfg[models.ConfigProjectID], option.WithCredentialsJSON(credential))
		if err != nil {
			return fmt.Errorf("new client: %w", err)
		}
		defer client.Close()

		topic := client.Topic(cfg[models.ConfigTopicID])
		defer topic.Stop()

		if _, err = client.CreateSubscription(ctx, cfg[models.ConfigSubscriptionID], pubsub.SubscriptionConfig{
			Topic: topic,
		}); err != nil {
			return fmt.Errorf("create subscription: %w", err)
		}

		return nil
	}

	admin, err := pubsublite.NewAdminClient(ctx, cfg[models.ConfigLocation], option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new admin: %w", err)
	}
	defer admin.Close()

	subscriptionPath := fmt.Sprintf(subscriptionPathFmt,
		cfg[models.ConfigProjectID], cfg[models.ConfigLocation], cfg[models.ConfigSubscriptionID])

	_, err = admin.CreateSubscription(ctx, pubsublite.SubscriptionConfig{
		Name: subscriptionPath,
		Topic: fmt.Sprintf(topicPathFmt,
			cfg[models.ConfigProjectID], cfg[models.ConfigLocation], cfg[models.ConfigTopicID]),
		DeliveryRequirement: pubsublite.DeliverImmediately, // can also be DeliverAfterStored
	})
	if err != nil {
		return fmt.Errorf("create subscription: %w", err)
	}

	return nil
}

func cleanupResources(
	ctx context.Context, cfg map[string]string, reservation string, subscriptionIDs []string, credential []byte,
) error {
	if cfg[models.ConfigLocation] == "" {
		client, err := pubsub.NewClient(ctx, cfg[models.ConfigProjectID], option.WithCredentialsJSON(credential))
		if err != nil {
			return fmt.Errorf("new client: %w", err)
		}
		defer client.Close()

		for i := range subscriptionIDs {
			if err = client.Subscription(subscriptionIDs[i]).Delete(ctx); err != nil {
				return fmt.Errorf("delete subscription %s: %w", subscriptionIDs[i], err)
			}
		}

		if err = client.Topic(cfg[models.ConfigTopicID]).Delete(ctx); err != nil {
			return fmt.Errorf("delete topic: %w", err)
		}

		return nil
	}

	admin, err := pubsublite.NewAdminClient(ctx, cfg[models.ConfigLocation], option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new admin: %w", err)
	}
	defer admin.Close()

	err = admin.DeleteSubscription(ctx, fmt.Sprintf(subscriptionPathFmt,
		cfg[models.ConfigProjectID], cfg[models.ConfigLocation], cfg[models.ConfigSubscriptionID]))
	if err != nil {
		return fmt.Errorf("delete subscription: %w", err)
	}

	err = admin.DeleteTopic(ctx, fmt.Sprintf(topicPathFmt,
		cfg[models.ConfigProjectID], cfg[models.ConfigLocation], cfg[models.ConfigTopicID]))
	if err != nil {
		return fmt.Errorf("delete topic: %w", err)
	}

	err = admin.DeleteReservation(ctx, reservation)
	if err != nil {
		return fmt.Errorf("delete topic: %w", err)
	}

	return nil
}
