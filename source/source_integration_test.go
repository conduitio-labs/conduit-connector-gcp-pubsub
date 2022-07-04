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
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/conduitio/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio/conduit-connector-gcp-pubsub/destination"
	"github.com/conduitio/conduit-connector-gcp-pubsub/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/jpillora/backoff"
	"go.uber.org/goleak"
	"google.golang.org/api/option"
)

func TestSource_Read(t *testing.T) { // nolint:gocyclo,nolintlint
	const (
		topicFmt = "acceptance-test-topic-%s"
		subFmt   = "acceptance-test-subscription-%s"

		ignoredTopFunction = "go.opencensus.io/stats/view.(*worker).start"
	)

	var (
		ctx            = context.Background()
		topicID        = fmt.Sprintf(topicFmt, uuid.New().String())
		subscriptionID = fmt.Sprintf(subFmt, uuid.New().String())
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

	cfg := map[string]string{
		models.ConfigPrivateKey:  privateKey,
		models.ConfigClientEmail: clientEmail,
		models.ConfigProjectID:   projectID,
	}

	srcCfg := map[string]string{
		models.ConfigPrivateKey:     privateKey,
		models.ConfigClientEmail:    clientEmail,
		models.ConfigProjectID:      projectID,
		models.ConfigSubscriptionID: subscriptionID,
	}

	dstCfg := map[string]string{
		models.ConfigPrivateKey:  privateKey,
		models.ConfigClientEmail: clientEmail,
		models.ConfigProjectID:   projectID,
		models.ConfigTopicID:     topicID,
		models.ConfigBatchSize:   os.Getenv("GCP_PUBSUB_BATCH_SIZE"),
		models.ConfigBatchDelay:  os.Getenv("GCP_PUBSUB_BATCH_DELAY"),
	}

	credential, err := getCredential(cfg)
	if err != nil {
		t.Error(err)
	}

	if err = createTopic(ctx, projectID, topicID, credential); err != nil {
		t.Errorf("create topic: %s", err.Error())
	}

	if err = createSubscription(ctx, projectID, topicID, subscriptionID, credential); err != nil {
		t.Errorf("create subscription: %s", err.Error())
	}

	defer func() {
		if err = cleanup(ctx, projectID, topicID, subscriptionID, credential); err != nil {
			t.Errorf("failed to cleanup resources: %s", err.Error())
		}
	}()

	t.Run("read empty", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreTopFunction(ignoredTopFunction))

		src := New()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(ctx, srcCfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		record, err := src.Read(ctx)
		if err != sdk.ErrBackoffRetry {
			t.Errorf("read error: got = %v, want = %v", err, sdk.ErrBackoffRetry)
		}

		if record.Key != nil {
			t.Error("record should be empty")
		}

		cancel()

		err = src.Teardown(context.Background())
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("configure, open and teardown", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreTopFunction(ignoredTopFunction))

		src := New()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(ctx, srcCfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		cancel()

		err = src.Teardown(context.Background())
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("publish and receive 1 message", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreTopFunction(ignoredTopFunction))

		const messagesCount = 1

		src := New()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(ctx, srcCfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount, dstCfg)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)

		for {
			record, err := readWithBackoffRetry(ctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(ctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)

			if len(records) == messagesCount {
				break
			}
		}

		cancel()

		err = src.Teardown(context.Background())
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compare(records, prepared)
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	t.Run("publish and receive 30 messages in a row with starts and stops", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreTopFunction(ignoredTopFunction))

		const (
			messagesCount = 30

			firstStopMessagesCount  = 10
			secondStopMessagesCount = 17
		)

		src := New()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(ctx, srcCfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount, dstCfg)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)

		for len(records) < firstStopMessagesCount {
			record, err := readWithBackoffRetry(ctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(ctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)
		}

		cancel()

		err = src.Teardown(context.Background())
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		err = src.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		for len(records) < secondStopMessagesCount {
			record, err := readWithBackoffRetry(ctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(ctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)
		}

		cancel()

		err = src.Teardown(context.Background())
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		err = src.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		for len(records) < messagesCount {
			record, err := readWithBackoffRetry(ctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(ctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)
		}

		cancel()

		err = src.Teardown(context.Background())
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compare(records, prepared)
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	t.Run("publish 2500 messages in a row", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreTopFunction(ignoredTopFunction))

		const messagesCount = 2500

		src := New()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(ctx, srcCfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount, dstCfg)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)

		for len(records) < messagesCount {
			record, err := readWithBackoffRetry(ctx, src)
			if err != nil {
				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(ctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)
		}

		cancel()

		err = src.Teardown(context.Background())
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

func createSubscription(ctx context.Context, projectID, topicID, subscriptionID string, credential []byte) error {
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

func cleanup(ctx context.Context, projectID, topicID, subscriptionID string, credential []byte) error {
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err = client.Subscription(subscriptionID).Delete(ctx); err != nil {
		return fmt.Errorf("delete subscription: %w", err)
	}

	if err = client.Topic(topicID).Delete(ctx); err != nil {
		return fmt.Errorf("delete topic: %w", err)
	}

	return nil
}

func prepareData(messagesCount int, dstCfg map[string]string) (map[string]struct{}, error) {
	const dataFmt = "{\"id\": %s}"

	dest := destination.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := dest.Configure(ctx, dstCfg)
	if err != nil {
		return nil, fmt.Errorf("configure: %s", err.Error())
	}

	err = dest.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open: %s", err.Error())
	}

	prepared := make(map[string]struct{}, messagesCount)

	for i := 0; i < messagesCount; i++ {
		data := fmt.Sprintf(dataFmt, strconv.Itoa(i))

		err = dest.WriteAsync(ctx, sdk.Record{
			Payload: sdk.RawData(data),
		}, func(ackErr error) error {
			if ackErr != nil {
				return fmt.Errorf("ack func: %s", ackErr.Error())
			}

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("write async: %s", err.Error())
		}

		prepared[data] = struct{}{}
	}

	err = dest.Flush(ctx)
	if err != nil {
		return nil, fmt.Errorf("flush: %s", err.Error())
	}

	cancel()

	err = dest.Teardown(context.Background())
	if err != nil {
		return nil, fmt.Errorf("teardown: %s", err.Error())
	}

	return prepared, nil
}

func compare(records []sdk.Record, prepared map[string]struct{}) error {
	for i := range records {
		payload := string(records[i].Payload.Bytes())

		if _, ok := prepared[payload]; !ok {
			return fmt.Errorf("no data in the map by data: %s", payload)
		}
	}

	return nil
}
