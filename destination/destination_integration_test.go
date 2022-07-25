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
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"go.uber.org/goleak"
	"google.golang.org/api/option"
)

func TestDestination_WriteAsync(t *testing.T) {
	const (
		hello    = "Hello, 世界"
		topicFmt = "acceptance-test-topic-%s"

		ignoredTopFunction = "go.opencensus.io/stats/view.(*worker).start"
	)

	var (
		ctx     = context.Background()
		topicID = fmt.Sprintf(topicFmt, uuid.New().String())
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

	defer func() {
		if err = deleteTopic(ctx, projectID, topicID, credential); err != nil {
			t.Errorf("failed to delete topic: %s", err.Error())
		}
	}()

	t.Run("success case", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreTopFunction(ignoredTopFunction))

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
			Payload: sdk.RawData(hello),
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

		err = dest.Teardown(context.Background())
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("item size exceeds bundle byte limit", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreTopFunction(ignoredTopFunction))

		const expectedErrMsg = "item size exceeds bundle byte limit"

		var sb strings.Builder

		// create string which length is more than 10mb
		for i := 0; i < 806600; i++ {
			sb.WriteString(hello)
		}

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
			Payload: sdk.RawData(bytes.NewBufferString(sb.String()).Bytes()),
		}, func(ackErr error) error {
			if ackErr.Error() != expectedErrMsg {
				t.Errorf("ack funk: %s", ackErr.Error())
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

		err = dest.Teardown(context.Background())
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
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

func deleteTopic(ctx context.Context, projectID, topicID string, credential []byte) error {
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err = client.Topic(topicID).Delete(ctx); err != nil {
		return fmt.Errorf("delete topic: %w", err)
	}

	return nil
}
