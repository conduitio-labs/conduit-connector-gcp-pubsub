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

package source

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/destination"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jpillora/backoff"
	"google.golang.org/api/option"
)

const (
	payload = "Hello, 世界"

	topicFmt        = "source-test-topic-%d"
	subscriptionFmt = "source-test-subscription-%d"

	topicPathFmt        = "projects/%s/locations/%s/topics/%s"
	subscriptionPathFmt = "projects/%s/locations/%s/subscriptions/%s"
	reservationPathFmt  = "projects/%s/locations/%s/reservations/reservation-%d"
)

func TestSource_Read(t *testing.T) { // nolint:gocyclo,nolintlint
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
	)

	credential, err := getCredential(cfg)
	if err != nil {
		t.Error(err)
	}

	if err = prepareResources(ctx, cfg, credential); err != nil {
		t.Errorf("prepare resource: %s", err.Error())
	}

	t.Cleanup(func() {
		if err = cleanupResources(ctx, cfg, credential); err != nil {
			t.Errorf("failed to cleanup resources: %s", err.Error())
		}
	})

	t.Run("read empty", func(t *testing.T) {
		src := New()

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(cctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		record, err := src.Read(cctx)
		if err != sdk.ErrBackoffRetry {
			t.Errorf("read error: got = %v, want = %v", err, sdk.ErrBackoffRetry)
		}

		if record.Key != nil {
			t.Error("record should be empty")
		}

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("configure, open and teardown", func(t *testing.T) {
		src := New()

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(cctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("publish and receive 1 message", func(t *testing.T) {
		const messagesCount = 1

		src := New()

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(cctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount, cfg)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		record, err := readWithBackoffRetry(cctx, src)
		if err != nil {
			t.Errorf("read: %s", err.Error())
		}

		err = src.Ack(cctx, nil)
		if err != nil {
			t.Errorf("ack: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)
		records = append(records, record)

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compare(records, prepared)
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	t.Run("publish and receive 1 message without acknowledgment", func(t *testing.T) {
		const messagesCount = 1

		src := New()

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(cctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount, cfg)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		_, err = readWithBackoffRetry(cctx, src)
		if err != nil {
			t.Errorf("read: %s", err.Error())
		}

		// there should be an acknowledgement of the message, but there isn't,
		// so trying to get the same message again (it will be received in a while)

		record, err := readWithBackoffRetry(cctx, src)
		if err != nil {
			t.Errorf("read: %s", err.Error())
		}

		err = src.Ack(cctx, nil)
		if err != nil {
			t.Errorf("ack: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)
		records = append(records, record)

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compare(records, prepared)
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	t.Run("publish and receive 30 messages in a row with starts and stops", func(t *testing.T) {
		const (
			messagesCount = 30

			firstStopMessagesCount  = 10
			secondStopMessagesCount = 17
		)

		src := New()

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(cctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount, cfg)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)

		for len(records) < firstStopMessagesCount {
			record, err := readWithBackoffRetry(cctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(cctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)
		}

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		cctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		for len(records) < secondStopMessagesCount {
			record, err := readWithBackoffRetry(cctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(cctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)
		}

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		cctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		for len(records) < messagesCount {
			record, err := readWithBackoffRetry(ctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(cctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)
		}

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compare(records, prepared)
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	t.Run("publish 2500 messages in a row", func(t *testing.T) {
		const messagesCount = 2500

		src := New()

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(cctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount, cfg)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)

		for len(records) < messagesCount {
			record, err := readWithBackoffRetry(cctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(cctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)
		}

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compare(records, prepared)
		if err != nil {
			t.Errorf(err.Error())
		}
	})
}

func TestSource_Read_Lite(t *testing.T) { // nolint:gocyclo,nolintlint
	var (
		ctx = context.Background()
		cfg = prepareConfigLite(t)

		err error
	)

	credential, err := getCredential(cfg)
	if err != nil {
		t.Error(err)
	}

	reservation := fmt.Sprintf(reservationPathFmt,
		cfg[models.ConfigProjectID], cfg[models.ConfigLocation], time.Now().Unix())

	if err = prepareResourcesLite(ctx, cfg, reservation, credential); err != nil {
		t.Errorf("prepare resource: %s", err.Error())
	}

	t.Cleanup(func() {
		if err = cleanupResourcesLite(ctx, cfg, reservation, credential); err != nil {
			t.Errorf("failed to cleanup resources: %s", err.Error())
		}
	})

	t.Run("read empty", func(t *testing.T) {
		src := New()

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(cctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		record, err := src.Read(cctx)
		if err != sdk.ErrBackoffRetry {
			t.Errorf("read error: got = %v, want = %v", err, sdk.ErrBackoffRetry)
		}

		if record.Key != nil {
			t.Error("record should be empty")
		}

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("configure, open and teardown", func(t *testing.T) {
		src := New()

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(cctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("publish and receive 1 message", func(t *testing.T) {
		const messagesCount = 1

		src := New()

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(cctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount, cfg)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		record, err := readWithBackoffRetry(cctx, src)
		if err != nil {
			t.Errorf("read: %s", err.Error())
		}

		err = src.Ack(cctx, nil)
		if err != nil {
			t.Errorf("ack: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)
		records = append(records, record)

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compare(records, prepared)
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	t.Run("publish and receive 1 message without acknowledgment", func(t *testing.T) {
		const messagesCount = 1

		src := New()

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(cctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount, cfg)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		_, err = readWithBackoffRetry(cctx, src)
		if err != nil {
			t.Errorf("read: %s", err.Error())
		}

		// There should be an acknowledgement of the message, but there isn't,
		// so trying to reconnect and receive this message again.
		// Lite subscriptions don't have an acknowledgment deadline and
		// the Pub/Sub Lite service doesn't redeliver unacknowledged messages over an open streaming connection.

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		record, err := readWithBackoffRetry(cctx, src)
		if err != nil {
			t.Errorf("read: %s", err.Error())
		}

		err = src.Ack(cctx, nil)
		if err != nil {
			t.Errorf("ack: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)
		records = append(records, record)

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compare(records, prepared)
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	t.Run("publish and receive 30 messages in a row with starts and stops", func(t *testing.T) {
		const (
			messagesCount = 30

			firstStopMessagesCount  = 10
			secondStopMessagesCount = 17
		)

		src := New()

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(cctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount, cfg)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)

		for len(records) < firstStopMessagesCount {
			record, err := readWithBackoffRetry(cctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(cctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)
		}

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		cctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		for len(records) < secondStopMessagesCount {
			record, err := readWithBackoffRetry(cctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(cctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)
		}

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		cctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		for len(records) < messagesCount {
			record, err := readWithBackoffRetry(ctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(cctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)
		}

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compare(records, prepared)
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	t.Run("publish 2500 messages in a row", func(t *testing.T) {
		const messagesCount = 2500

		src := New()

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(cctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(cctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount, cfg)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)

		for len(records) < messagesCount {
			record, err := readWithBackoffRetry(cctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(cctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)
		}

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compare(records, prepared)
		if err != nil {
			t.Errorf(err.Error())
		}
	})
}

func readWithBackoffRetry(ctx context.Context, src sdk.Source) (sdk.Record, error) {
	b := &backoff.Backoff{
		Factor: 2,
		Min:    time.Millisecond * 100,
		Max:    time.Second,
	}

	for {
		got, err := src.Read(ctx)

		if errors.Is(err, sdk.ErrBackoffRetry) {
			select {
			case <-ctx.Done():
				return sdk.Record{}, ctx.Err()
			case <-time.After(b.Duration()):
				continue
			}
		}

		return got, err
	}
}

func prepareConfig(t *testing.T) map[string]string {
	privateKey := os.Getenv("GCP_PUBSUB_PRIVATE_KEY")
	if privateKey == "" {
		t.Skip("GCP_PUBSUB_PRIVATE_KEY env var must be set")

		return nil
	}

	clientEmail := os.Getenv("GCP_PUBSUB_CLIENT_EMAIL")
	if clientEmail == "" {
		t.Skip("GCP_PUBSUB_CLIENT_EMAIL env var must be set")

		return nil
	}

	projectID := os.Getenv("GCP_PUBSUB_PROJECT_ID")
	if projectID == "" {
		t.Skip("GCP_PUBSUB_PROJECT_ID env var must be set")

		return nil
	}

	return map[string]string{
		models.ConfigPrivateKey:     privateKey,
		models.ConfigClientEmail:    clientEmail,
		models.ConfigProjectID:      projectID,
		models.ConfigTopicID:        fmt.Sprintf(topicFmt, time.Now().Unix()),
		models.ConfigBatchSize:      os.Getenv("GCP_PUBSUB_BATCH_SIZE"),
		models.ConfigBatchDelay:     os.Getenv("GCP_PUBSUB_BATCH_DELAY"),
		models.ConfigSubscriptionID: fmt.Sprintf(subscriptionFmt, time.Now().Unix()),
	}
}

func prepareConfigLite(t *testing.T) map[string]string {
	location := os.Getenv("GCP_PUBSUB_LOCATION")
	if location == "" {
		t.Skip("GCP_PUBSUB_LOCATION env var must be set")

		return nil
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

	defer topic.Stop()

	if _, err = client.CreateSubscription(ctx, cfg[models.ConfigSubscriptionID], pubsub.SubscriptionConfig{
		Topic: topic,
	}); err != nil {
		return fmt.Errorf("create subscription: %w", err)
	}

	return nil
}

func cleanupResources(ctx context.Context, cfg map[string]string, credential []byte) error {
	client, err := pubsub.NewClient(ctx, cfg[models.ConfigProjectID], option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err = client.Subscription(cfg[models.ConfigSubscriptionID]).Delete(ctx); err != nil {
		return fmt.Errorf("delete subscription: %w", err)
	}

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

	subscriptionPath := fmt.Sprintf(subscriptionPathFmt,
		cfg[models.ConfigProjectID], cfg[models.ConfigLocation], cfg[models.ConfigSubscriptionID])

	_, err = admin.CreateSubscription(ctx, pubsublite.SubscriptionConfig{
		Name:                subscriptionPath,
		Topic:               topicPath,
		DeliveryRequirement: pubsublite.DeliverImmediately, // can also be DeliverAfterStored
	})
	if err != nil {
		return fmt.Errorf("create subscription: %w", err)
	}

	return nil
}

func cleanupResourcesLite(ctx context.Context, cfg map[string]string, reservation string, credential []byte) error {
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

func prepareData(messagesCount int, cfg map[string]string) (map[string]sdk.Record, error) {
	const (
		dataFmt          = "{\"id\": %d}"
		metadataKey      = "metadata"
		metadataValueFmt = "metadata_%d"
	)

	dest := destination.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := dest.Configure(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("configure: %s", err.Error())
	}

	err = dest.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open: %s", err.Error())
	}

	prepared := make(map[string]sdk.Record, messagesCount)

	for i := 0; i < messagesCount; i++ {
		data := fmt.Sprintf(dataFmt, i)

		r := sdk.Record{
			Metadata: map[string]string{
				metadataKey: fmt.Sprintf(metadataValueFmt, i),
			},
			Payload: sdk.RawData(data),
		}

		err = dest.WriteAsync(ctx, r, func(ackErr error) error {
			if ackErr != nil {
				return fmt.Errorf("ack func: %s", ackErr.Error())
			}

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("write async: %s", err.Error())
		}

		prepared[data] = r
	}

	err = dest.Flush(ctx)
	if err != nil {
		return nil, fmt.Errorf("flush: %s", err.Error())
	}

	err = dest.Teardown(ctx)
	if err != nil {
		return nil, fmt.Errorf("teardown: %s", err.Error())
	}

	return prepared, nil
}

func compare(records []sdk.Record, prepared map[string]sdk.Record) error {
	for i := range records {
		pr, ok := prepared[string(records[i].Payload.Bytes())]
		if !ok {
			return fmt.Errorf("no data in the map by data: %s", payload)
		}

		if !reflect.DeepEqual(records[i].Metadata, pr.Metadata) {
			return fmt.Errorf("expected metadata \"%+v\", got \"%+v\"", pr.Metadata, records[i].Metadata)
		}

		if !bytes.Equal(records[i].Payload.Bytes(), pr.Payload.Bytes()) {
			return fmt.Errorf("expected payload \"%s\", got \"%s\"",
				string(pr.Payload.Bytes()), string(records[i].Payload.Bytes()))
		}
	}

	return nil
}
