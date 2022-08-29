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
	"github.com/matryer/is"
	"google.golang.org/api/option"
)

const (
	payload = "Hello, 世界"
)

func TestDestination_WriteSuccess(t *testing.T) {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		dest        = NewDestination()
		cfg         = prepareConfig(t)
		is          = is.New(t)
	)

	prepareTest(t, is, cfg)

	err := dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	records := []sdk.Record{
		{
			Payload: sdk.Change{After: sdk.RawData(payload)},
		},
	}

	n := 0
	n, err = dest.Write(ctx, records)
	is.NoErr(err)
	is.Equal(n, len(records))

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_WriteFail(t *testing.T) {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		dest        = NewDestination()
		cfg         = prepareConfig(t)
		is          = is.New(t)
	)

	prepareTest(t, is, cfg)

	err := dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	// make the payload 10 Mb, so that the message sent with the payload is larger
	// for Lite service it must be more than 3,5 Mb
	p := make([]byte, 10*1024*1024)
	for i := range p {
		p[i] = '!'
	}

	records := []sdk.Record{{
		Payload: sdk.Change{After: sdk.RawData(p)},
	}}

	n := 0
	n, err = dest.Write(ctx, records)
	is.Equal(n, 0)
	if cfg[models.ConfigLocation] == "" {
		is.Equal(err.Error(), "publish message: item size exceeds bundle byte limit")
	} else {
		is.Equal(err.Error(), "publish message: pubsublite: serialized message size is 10485765 bytes: "+
			"maximum allowed message size is MaxPublishRequestBytes (3670016)")
	}

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
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
		models.ConfigPrivateKey:  privateKey,
		models.ConfigClientEmail: clientEmail,
		models.ConfigProjectID:   projectID,
		models.ConfigLocation:    os.Getenv("GCP_PUBSUB_LOCATION"),
		models.ConfigTopicID:     fmt.Sprintf("destination-test-topic-%d", time.Now().Unix()),
	}
}

func prepareTest(t *testing.T, is *is.I, cfg map[string]string) {
	credential, err := getCredential(cfg)
	is.NoErr(err)

	if cfg[models.ConfigLocation] == "" {
		err = prepareResources(cfg, credential)
		is.NoErr(err)

		t.Cleanup(func() {
			err = cleanupResources(cfg, credential)
			is.NoErr(err)
		})

		return
	}

	reservation := fmt.Sprintf("projects/%s/locations/%s/reservations/reservation-%d",
		cfg[models.ConfigProjectID], cfg[models.ConfigLocation], time.Now().Unix())

	topicPath := fmt.Sprintf("projects/%s/locations/%s/topics/%s",
		cfg[models.ConfigProjectID], cfg[models.ConfigLocation], cfg[models.ConfigTopicID])

	err = prepareResourcesLite(cfg, reservation, topicPath, credential)
	is.NoErr(err)

	t.Cleanup(func() {
		err = cleanupResourcesLite(cfg, reservation, topicPath, credential)
		is.NoErr(err)
	})
}

func getCredential(src map[string]string) ([]byte, error) {
	return config.General{
		PrivateKey:  src[models.ConfigPrivateKey],
		ClientEmail: src[models.ConfigClientEmail],
		ProjectID:   src[models.ConfigProjectID],
	}.Marshal()
}

func prepareResources(cfg map[string]string, credential []byte) error {
	var ctx = context.Background()

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

func cleanupResources(cfg map[string]string, credential []byte) error {
	var ctx = context.Background()

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

func prepareResourcesLite(
	cfg map[string]string,
	reservation, topicPath string,
	credential []byte,
) error {
	const gib = 1 << 30

	var ctx = context.Background()

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
		Name:                       topicPath,
		PartitionCount:             1,
		PublishCapacityMiBPerSec:   4,
		SubscribeCapacityMiBPerSec: 4,
		PerPartitionBytes:          30 * gib,
		ThroughputReservation:      reservation,
		RetentionDuration:          pubsublite.InfiniteRetention,
	}

	_, err = admin.CreateTopic(ctx, topicConfig)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}

	return nil
}

func cleanupResourcesLite(
	cfg map[string]string,
	reservation, topicPath string,
	credential []byte,
) error {
	var ctx = context.Background()

	admin, err := pubsublite.NewAdminClient(ctx, cfg[models.ConfigLocation], option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new admin: %w", err)
	}
	defer admin.Close()

	err = admin.DeleteTopic(ctx, topicPath)
	if err != nil {
		return fmt.Errorf("delete topic: %w", err)
	}

	err = admin.DeleteReservation(ctx, reservation)
	if err != nil {
		return fmt.Errorf("delete topic: %w", err)
	}

	return nil
}
