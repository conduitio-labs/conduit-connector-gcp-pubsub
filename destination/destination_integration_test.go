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
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/conduitio/conduit-connector-gcp-pubsub/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestDestination_WriteAsync(t *testing.T) {
	const hello = "Hello, 世界"

	t.Run("success case", func(t *testing.T) {
		dest := New()

		ctx := context.Background()

		cfg, err := prepareConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

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

		err = dest.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
		}
	})

	t.Run("item size exceeds bundle byte limit", func(t *testing.T) {
		const expectedErrMsg = "item size exceeds bundle byte limit"

		var sb strings.Builder

		// create string which length is more than 10mb
		for i := 0; i < 806600; i++ {
			sb.WriteString(hello)
		}

		dest := New()

		ctx := context.Background()

		cfg, err := prepareConfig()
		if err != nil {
			t.Log(err)
			t.Skip()
		}

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

		err = dest.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown: %s", err.Error())
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
