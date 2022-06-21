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
	"reflect"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/conduitio/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio/conduit-connector-gcp-pubsub/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/api/option"
)

func TestSource_Read(t *testing.T) { // nolint:gocyclo,nolintlint
	t.Run("read, ack and teardown without gcp pubsub client initialization", func(t *testing.T) {
		pubsubSource := New()

		ctx := context.Background()

		cfg, err := prepareConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

		err = pubsubSource.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		_, err = pubsubSource.Read(ctx)
		if err != errPubsubIsNil {
			t.Errorf("read: got = %v, want = %v", err, errPubsubIsNil)
		}

		err = pubsubSource.Ack(ctx, nil)
		if err != errPubsubIsNil {
			t.Errorf("ack: got = %v, want = %v", err, errPubsubIsNil)
		}

		err = pubsubSource.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("read empty", func(t *testing.T) {
		pubsubSource := New()

		ctx := context.Background()

		cfg, err := prepareConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

		err = pubsubSource.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = pubsubSource.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		record, err := pubsubSource.Read(ctx)
		if err != sdk.ErrBackoffRetry {
			t.Errorf("read error: got = %v, want = %v", err, sdk.ErrBackoffRetry)
		}

		if record.Key != nil {
			t.Error("record should be empty")
		}

		err = pubsubSource.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("configure, open and teardown", func(t *testing.T) {
		pubsubSource := New()

		ctx := context.Background()

		cfg, err := prepareConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

		err = pubsubSource.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = pubsubSource.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		err = pubsubSource.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("publish and receive 1 message", func(t *testing.T) {
		const messagesCount = 1

		cfg, err := prepareConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

		pubsubSource := New()

		ctx := context.Background()

		err = pubsubSource.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = pubsubSource.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		dataMap, err := generateAndPublish(ctx, cfg, messagesCount)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)

		for {
			record, err := pubsubSource.Read(ctx)
			if err != nil {
				if err == sdk.ErrBackoffRetry {
					continue
				}

				t.Errorf("read: %s", err.Error())
			}

			err = pubsubSource.Ack(ctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)

			if len(records) == messagesCount {
				break
			}
		}

		err = pubsubSource.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compareResults(records, dataMap)
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

		cfg, err := prepareConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

		pubsubSource := New()

		ctx := context.Background()

		err = pubsubSource.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = pubsubSource.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		dataMap, err := generateAndPublish(ctx, cfg, messagesCount)
		if err != nil {
			t.Errorf("generate and publish: %s", err.Error())
		}

		records := make([]sdk.Record, 0, messagesCount)

		for {
			record, err := pubsubSource.Read(ctx)
			if err != nil {
				if err == sdk.ErrBackoffRetry {
					continue
				}

				t.Errorf("read: %s", err.Error())
			}

			err = pubsubSource.Ack(ctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)

			if len(records) == firstStopMessagesCount {
				break
			}
		}

		err = pubsubSource.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = pubsubSource.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		for {
			record, err := pubsubSource.Read(ctx)
			if err != nil {
				if err == sdk.ErrBackoffRetry {
					continue
				}

				t.Errorf("read: %s", err.Error())
			}

			err = pubsubSource.Ack(ctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)

			if len(records) == secondStopMessagesCount {
				break
			}
		}

		err = pubsubSource.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = pubsubSource.Open(ctx, nil)
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

			record, err := pubsubSource.Read(ctx)
			if err != nil {
				if err == sdk.ErrBackoffRetry {
					continue
				}

				t.Errorf("read: %s", err.Error())
			}

			err = pubsubSource.Ack(ctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)

			if len(records) == messagesCount {
				break
			}
		}

		err = pubsubSource.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compareResults(records, dataMap)
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	t.Run("publish 2500 messages in a row with 500 additional requests", func(t *testing.T) {
		const messagesCount = 2500

		var additionalRequestsCount = 500

		cfg, err := prepareConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

		pubsubSource := New()

		ctx := context.Background()

		err = pubsubSource.Configure(ctx, cfg)
		if err != nil {
			t.Errorf("configure: %s", err.Error())
		}

		err = pubsubSource.Open(ctx, nil)
		if err != nil {
			t.Errorf("open: %s", err.Error())
		}

		dataMap, err := generateAndPublish(ctx, cfg, messagesCount)
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

			record, err := pubsubSource.Read(ctx)
			if err != nil {
				if err == sdk.ErrBackoffRetry {
					continue
				}

				t.Errorf("read: %s", err.Error())
			}

			err = pubsubSource.Ack(ctx, nil)
			if err != nil {
				t.Errorf("ack: %s", err.Error())
			}

			records = append(records, record)

			if len(records) == messagesCount {
				break
			}
		}

		err = pubsubSource.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}

		err = compareResults(records, dataMap)
		if err != nil {
			t.Errorf(err.Error())
		}
	})
}

func prepareConfig() (map[string]string, error) {
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

func initClient(ctx context.Context, cfg map[string]string) (*pubsub.Client, error) {
	cfgSrc, err := config.ParseSource(cfg)
	if err != nil {
		return nil, fmt.Errorf("parse source config: %w", err)
	}

	credentialJSON, err := cfgSrc.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal creadential json: %w", err)
	}

	client, err := pubsub.NewClient(ctx, cfgSrc.ProjectID, option.WithCredentialsJSON(credentialJSON))
	if err != nil {
		return nil, fmt.Errorf("init new client: %w", err)
	}

	return client, nil
}

func generateAndPublish(ctx context.Context, cfg map[string]string, messagesCount int) (map[string][]byte, error) {
	cli, err := initClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("init pub client: %w", err)
	}
	defer cli.Close()

	dataMap := make(map[string][]byte, messagesCount)

	for i := 0; i < messagesCount; i++ {
		data := []byte(fmt.Sprintf("{\"i\": \"%d\"}", i))

		id, err := publish(ctx, cli, data)
		if err != nil {
			return nil, fmt.Errorf("publish: %w", err)
		}

		dataMap[id] = data
	}

	return dataMap, nil
}

func publish(ctx context.Context, cli *pubsub.Client, data []byte) (string, error) {
	topicID := os.Getenv("GCP_PUBSUB_TOPIC_ID")
	if topicID == "" {
		return "", errors.New("GCP_PUBSUB_TOPIC_ID env var must be set")
	}

	msgID, err := cli.Topic(topicID).Publish(ctx, &pubsub.Message{
		Data: data,
	}).Get(ctx)
	if err != nil {
		return "", fmt.Errorf("publish message")
	}

	return msgID, nil
}

func compareResults(records []sdk.Record, dataMap map[string][]byte) error {
	for i := range records {
		id := string(records[i].Position)

		data, ok := dataMap[id]
		if !ok {
			return fmt.Errorf("no data in the map by id: %s", id)
		}

		err := compareResult(records[i], id, data)
		if err != nil {
			return fmt.Errorf("compare result: %w", err)
		}
	}

	return nil
}

func compareResult(record sdk.Record, id string, data []byte) error {
	if string(record.Position) != id {
		return fmt.Errorf("position: got = %v, want = %v", string(record.Position), id)
	}

	if !reflect.DeepEqual(record.Key, sdk.StructuredData{idKey: id}) {
		return fmt.Errorf("key: got = %v, want = %v", string(record.Key.Bytes()), id)
	}

	if record.Metadata[actionKey] != insertValue {
		return fmt.Errorf("action: got = %v, want = %v", record.Metadata[actionKey], insertValue)
	}

	if !reflect.DeepEqual(record.Payload.Bytes(), data) {
		return fmt.Errorf("payload: got = %v, want = %v", string(record.Payload.Bytes()), string(data))
	}

	return nil
}
