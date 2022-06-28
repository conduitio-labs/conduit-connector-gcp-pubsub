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

	"github.com/conduitio/conduit-connector-gcp-pubsub/destination"
	"github.com/conduitio/conduit-connector-gcp-pubsub/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestSource_Read(t *testing.T) { // nolint:gocyclo,nolintlint
	t.Run("read empty", func(t *testing.T) {
		src := New()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg, err := prepareSrcConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

		err = src.Configure(ctx, cfg)
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
		src := New()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg, err := prepareSrcConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

		err = src.Configure(ctx, cfg)
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
		const messagesCount = 1

		cfg, err := prepareSrcConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

		src := New()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)

		for {
			record, err := src.Read(ctx)
			if err != nil {
				if err == sdk.ErrBackoffRetry {
					continue
				}

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

	t.Run("publish and receive 30 messages in a row with starts and stops, "+
		"and with additional 20 requests", func(t *testing.T) {
		const (
			messagesCount = 30

			firstStopMessagesCount  = 10
			secondStopMessagesCount = 17
		)

		var additionalRequestsCount = 20

		cfg, err := prepareSrcConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

		src := New()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)

		for {
			record, err := src.Read(ctx)
			if err != nil {
				if err == sdk.ErrBackoffRetry {
					continue
				}

				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(ctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)

			if len(records) == firstStopMessagesCount {
				break
			}
		}

		cancel()

		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = src.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		for {
			record, err := src.Read(ctx)
			if err != nil {
				if err == sdk.ErrBackoffRetry {
					continue
				}

				t.Errorf("read: %s", err.Error())
			}

			err = src.Ack(ctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)

			if len(records) == secondStopMessagesCount {
				break
			}
		}

		cancel()

		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		err = src.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = src.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		i := 0
		for {
			// when all messages are received,
			// make additionalRequestsCount additional queries to check that there are no more messages
			if len(records) == messagesCount {
				i++

				if additionalRequestsCount == i {
					break
				}
			}

			record, err := src.Read(ctx)
			if err != nil {
				if err == sdk.ErrBackoffRetry {
					continue
				}

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

	t.Run("publish 2500 messages in a row with 500 additional requests", func(t *testing.T) {
		const messagesCount = 2500

		var additionalRequestsCount = 500

		cfg, err := prepareSrcConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

		src := New()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = src.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = src.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		prepared, err := prepareData(messagesCount)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)

		i := 0
		for {
			// when all messages are received,
			// make additionalRequestsCount additional queries to check that there are no more messages
			if len(records) == messagesCount {
				i++

				if additionalRequestsCount == i {
					break
				}
			}

			record, err := src.Read(ctx)
			if err != nil {
				if err == sdk.ErrBackoffRetry {
					continue
				}

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
}

func prepareSrcConfig() (map[string]string, error) {
	privateKey := os.Getenv("GCP_PUBSUB_PRIVATE_KEY")
	if privateKey == "" {
		return map[string]string{}, errors.New("GCP_PUBSUB_PRIVATE_KEY env var must be set")
	}

	clientEmail := os.Getenv("GCP_PUBSUB_CLIENT_EMAIL")
	if clientEmail == "" {
		return map[string]string{}, errors.New("GCP_PUBSUB_CLIENT_EMAIL env var must be set")
	}

	projectID := os.Getenv("GCP_PUBSUB_PROJECT_ID")
	if projectID == "" {
		return map[string]string{}, errors.New("GCP_PUBSUB_PROJECT_ID env var must be set")
	}

	subscriptionID := os.Getenv("GCP_PUBSUB_SUBSCRIPTION_ID")
	if subscriptionID == "" {
		return map[string]string{}, errors.New("GCP_PUBSUB_SUBSCRIPTION_ID env var must be set")
	}

	return map[string]string{
		models.ConfigPrivateKey:     privateKey,
		models.ConfigClientEmail:    clientEmail,
		models.ConfigProjectID:      projectID,
		models.ConfigSubscriptionID: subscriptionID,
	}, nil
}

func prepareDestConfig() (map[string]string, error) {
	privateKey := os.Getenv("GCP_PUBSUB_PRIVATE_KEY")
	if privateKey == "" {
		return map[string]string{}, errors.New("GCP_PUBSUB_PRIVATE_KEY env var must be set")
	}

	clientEmail := os.Getenv("GCP_PUBSUB_CLIENT_EMAIL")
	if clientEmail == "" {
		return map[string]string{}, errors.New("GCP_PUBSUB_CLIENT_EMAIL env var must be set")
	}

	projectID := os.Getenv("GCP_PUBSUB_PROJECT_ID")
	if projectID == "" {
		return map[string]string{}, errors.New("GCP_PUBSUB_PROJECT_ID env var must be set")
	}

	topicID := os.Getenv("GCP_PUBSUB_TOPIC_ID")
	if topicID == "" {
		return map[string]string{}, errors.New("GCP_PUBSUB_TOPIC_ID env var must be set")
	}

	return map[string]string{
		models.ConfigPrivateKey:  privateKey,
		models.ConfigClientEmail: clientEmail,
		models.ConfigProjectID:   projectID,
		models.ConfigTopicID:     topicID,
		models.ConfigBatchSize:   os.Getenv("GCP_PUBSUB_BATCH_SIZE"),
		models.ConfigBatchDelay:  os.Getenv("GCP_PUBSUB_BATCH_DELAY"),
	}, nil
}

func prepareData(messagesCount int) (map[string]struct{}, error) {
	const dataFmt = "{\"id\": %s}"

	dest := destination.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := prepareDestConfig()
	if err != nil {
		return nil, err
	}

	err = dest.Configure(ctx, cfg)
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
