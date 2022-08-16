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

package destination

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
	"google.golang.org/api/option"
)

const (
	payload = "Hello, 世界"

	topicFmt           = "destination-test-topic-%d"
	topicPathFmt       = "projects/%s/locations/%s/topics/%s"
	reservationPathFmt = "projects/%s/locations/%s/reservations/reservation-%d"
)

func TestDestination_WriteAsync(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
	)

	credential, err := getCredential(cfg)
	if err != nil {
		t.Error(err)
	}

	if err = prepareResources(ctx, cfg, credential); err != nil {
		t.Errorf("create topic: %s", err.Error())
	}

	t.Cleanup(func() {
		if err = cleanupResources(ctx, cfg, credential); err != nil {
			t.Errorf("failed to delete topic: %s", err.Error())
		}
	})

	t.Run("success case", func(t *testing.T) {
		dest := New()

		err = dest.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = dest.Open(ctx)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		err = dest.WriteAsync(ctx, sdk.Record{
			Payload: sdk.RawData(payload),
		}, func(ackErr error) error {
			if ackErr != nil {
				t.Errorf("ack func: %s", ackErr.Error())
			}

			return nil
		})
		if err != nil {
			t.Errorf("write async: %s", err.Error())
		}

		err = dest.Flush(ctx)
		if err != nil {
			t.Errorf("flush: %s", err.Error())
		}

		err = dest.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("item size exceeds bundle byte limit", func(t *testing.T) {
		dest := New()

		err = dest.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = dest.Open(ctx)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		p := make([]byte, 10*1024*1024+1)
		for i := range p {
			p[i] = '!'
		}

		err = dest.WriteAsync(ctx, sdk.Record{
			Payload: sdk.RawData(p),
		}, func(ackErr error) error {
			if ackErr == nil {
				t.Error("ack funk must return an error")
			}

			return nil
		})
		if err != nil {
			t.Errorf("write async: %s", err.Error())
		}

		err = dest.Flush(ctx)
		if err != nil {
			t.Errorf("flush: %s", err.Error())
		}

		err = dest.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})
}

func TestDestination_WriteAsync_Lite(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfigLite(t)
	)

	credential, err := getCredential(cfg)
	if err != nil {
		t.Error(err)
	}

	reservation := fmt.Sprintf(reservationPathFmt,
		cfg[models.ConfigProjectID], cfg[models.ConfigLocation], time.Now().UnixNano())

	if err = prepareResourcesLite(ctx, cfg, reservation, credential); err != nil {
		t.Errorf("create topic: %s", err.Error())
	}

	t.Cleanup(func() {
		if err = cleanupResourcesLite(ctx, cfg, reservation, credential); err != nil {
			t.Errorf("failed to delete topic: %s", err.Error())
		}
	})

	t.Run("success case", func(t *testing.T) {
		dest := New()

		err = dest.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = dest.Open(ctx)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		err = dest.WriteAsync(ctx, sdk.Record{
			Payload: sdk.RawData(payload),
		}, func(ackErr error) error {
			if ackErr != nil {
				t.Errorf("ack func: %s", ackErr.Error())
			}

			return nil
		})
		if err != nil {
			t.Errorf("write async: %s", err.Error())
		}

		err = dest.Flush(ctx)
		if err != nil {
			t.Errorf("flush: %s", err.Error())
		}

		err = dest.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("item size exceeds bundle byte limit", func(t *testing.T) {
		dest := New()

		err = dest.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = dest.Open(ctx)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		p := make([]byte, 3.5*1024*1024+1)
		for i := range p {
			p[i] = '!'
		}

		err = dest.WriteAsync(ctx, sdk.Record{
			Payload: sdk.RawData(p),
		}, func(ackErr error) error {
			if ackErr == nil {
				t.Error("ack funk must return an error")
			}

			return nil
		})
		if err != nil {
			t.Errorf("write async: %s", err.Error())
		}

		err = dest.Flush(ctx)
		if err != nil {
			t.Errorf("flush: %s", err.Error())
		}

		err = dest.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
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
		models.ConfigBatchSize:   os.Getenv("GCP_PUBSUB_BATCH_SIZE"),
		models.ConfigBatchDelay:  os.Getenv("GCP_PUBSUB_BATCH_DELAY"),
	}
}

func prepareConfigLite(t *testing.T) map[string]string {
	location := os.Getenv("GCP_PUBSUB_LOCATION")
	if location == "" {
		t.Skip("GCP_PUBSUB_LOCATION env var must be set")
	}

	cfg := prepareConfig(t)
	cfg[models.ConfigLocation] = location

	return cfg
}

func getCredential(src map[string]string) ([]byte, error) {
	return config.General{
		PrivateKey:  src[models.ConfigPrivateKey],
		ClientEmail: src[models.ConfigClientEmail],
		ProjectID:   src[models.ConfigProjectID],
	}.Marshal()
}

func prepareResources(ctx context.Context, cfg map[string]string, credential []byte) error {
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

func cleanupResources(ctx context.Context, cfg map[string]string, credential []byte) error {
	client, err := pubsub.NewClient(ctx, cfg[models.ConfigProjectID], option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err = client.Topic(cfg[models.ConfigTopicID]).Delete(ctx); err != nil {
		return fmt.Errorf("delete topic: %w", err)
	}

	return nil
}

func prepareResourcesLite(ctx context.Context, cfg map[string]string, reservation string, credential []byte) error {
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

	topicConfig := pubsublite.TopicConfig{
		Name: fmt.Sprintf(topicPathFmt,
			cfg[models.ConfigProjectID], cfg[models.ConfigLocation], cfg[models.ConfigTopicID]),
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

func cleanupResourcesLite(ctx context.Context, cfg map[string]string, reservation string, credential []byte) error {
	admin, err := pubsublite.NewAdminClient(ctx, cfg[models.ConfigLocation], option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new admin: %w", err)
	}
	defer admin.Close()

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
