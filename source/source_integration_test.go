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
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/destination"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"google.golang.org/api/option"
)

func TestSource_NoRead(t *testing.T) {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		src         = NewSource()
		cfg         = prepareConfig(t)
		is          = is.New(t)
	)

	prepareTest(t, is, cfg)

	err := src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_ReadEmpty(t *testing.T) {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		src         = NewSource()
		cfg         = prepareConfig(t)
		is          = is.New(t)
	)

	prepareTest(t, is, cfg)

	err := src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	_, err = src.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_ReadOneRecord(t *testing.T) {
	const recordsCount = 1

	var (
		ctx, cancel = context.WithCancel(context.Background())
		src         = NewSource()
		cfg         = prepareConfig(t)
		is          = is.New(t)
	)

	prepareTest(t, is, cfg)

	err := src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	prepared := prepareData(is, recordsCount, cfg)

	record, err := ReadWithBackoffRetry(ctx, src)
	is.NoErr(err)

	err = src.Ack(ctx, nil)
	is.NoErr(err)

	cancel()

	records := make([]opencdc.Record, 0, recordsCount)
	records = append(records, record)

	err = src.Teardown(context.Background())
	is.NoErr(err)

	compare(is, records, prepared)
}

func TestSource_ReadRecordsWithStops(t *testing.T) {
	const (
		recordsCount            = 30
		firstStopIteratorCount  = 10
		secondStopIteratorCount = 17
	)

	var (
		ctx, cancel = context.WithCancel(context.Background())
		src         = NewSource()
		cfg         = prepareConfig(t)
		is          = is.New(t)
	)

	prepareTest(t, is, cfg)

	err := src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	prepared := prepareData(is, recordsCount, cfg)

	// read the first firstStopIteratorCount records and stop
	records := make([]opencdc.Record, 0, recordsCount)
	for len(records) < firstStopIteratorCount {
		record, err := ReadWithBackoffRetry(ctx, src)
		is.NoErr(err)

		err = src.Ack(ctx, nil)
		is.NoErr(err)

		records = append(records, record)
	}

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)

	ctx, cancel = context.WithCancel(context.Background())

	err = src.Open(ctx, nil)
	is.NoErr(err)

	// read the second secondStopIteratorCount records and stop
	for len(records) < secondStopIteratorCount {
		record, err := ReadWithBackoffRetry(ctx, src)
		is.NoErr(err)

		err = src.Ack(ctx, nil)
		is.NoErr(err)

		records = append(records, record)
	}

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)

	ctx, cancel = context.WithCancel(context.Background())

	err = src.Open(ctx, nil)
	is.NoErr(err)

	// read rest of the records
	for len(records) < recordsCount {
		record, err := ReadWithBackoffRetry(ctx, src)
		is.NoErr(err)

		err = src.Ack(ctx, nil)
		is.NoErr(err)

		records = append(records, record)
	}

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)

	compare(is, records, prepared)
}

func TestSource_ReadALotOfRecords(t *testing.T) {
	const recordsCount = 500

	var (
		ctx, cancel = context.WithCancel(context.Background())
		src         = NewSource()
		cfg         = prepareConfig(t)
		is          = is.New(t)
	)

	prepareTest(t, is, cfg)

	err := src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	prepared := prepareData(is, recordsCount, cfg)

	records := make([]opencdc.Record, 0, recordsCount)

	for len(records) < recordsCount {
		record, err := ReadWithBackoffRetry(ctx, src)
		is.NoErr(err)

		err = src.Ack(ctx, nil)
		is.NoErr(err)

		records = append(records, record)
	}

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)

	compare(is, records, prepared)
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

	timeNow := time.Now().Unix()

	return map[string]string{
		models.ConfigPrivateKey:     privateKey,
		models.ConfigClientEmail:    clientEmail,
		models.ConfigProjectID:      projectID,
		models.ConfigLocation:       os.Getenv("GCP_PUBSUB_LOCATION"),
		models.ConfigTopicID:        fmt.Sprintf("source-test-topic-%d", timeNow),
		models.ConfigSubscriptionID: fmt.Sprintf("source-test-topic-%d-sub", timeNow),
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

	subscriptionPath := fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s",
		cfg[models.ConfigProjectID], cfg[models.ConfigLocation], cfg[models.ConfigSubscriptionID])

	err = prepareResourcesLite(cfg, reservation, topicPath, subscriptionPath, credential)
	is.NoErr(err)

	t.Cleanup(func() {
		err = cleanupResourcesLite(cfg, reservation, topicPath, subscriptionPath, credential)
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

	defer topic.Stop()

	if _, err = client.CreateSubscription(ctx, cfg[models.ConfigSubscriptionID], pubsub.SubscriptionConfig{
		Topic: topic,
	}); err != nil {
		return fmt.Errorf("create subscription: %w", err)
	}

	return nil
}

func cleanupResources(cfg map[string]string, credential []byte) error {
	var ctx = context.Background()

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

func prepareResourcesLite(
	cfg map[string]string,
	reservation, topicPath, subscriptionPath string,
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
		PartitionCount:             1,        // Must be at least 1.
		PublishCapacityMiBPerSec:   4,        // Must be 4-16 MiB/s.
		SubscribeCapacityMiBPerSec: 4,        // Must be 4-32 MiB/s.
		PerPartitionBytes:          30 * gib, // Must be 30 GiB-10 TiB.
		ThroughputReservation:      reservation,
		RetentionDuration:          pubsublite.InfiniteRetention,
	}

	_, err = admin.CreateTopic(ctx, topicConfig)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}

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

func cleanupResourcesLite(
	cfg map[string]string,
	reservation, topicPath, subscriptionPath string,
	credential []byte,
) error {
	var ctx = context.Background()

	admin, err := pubsublite.NewAdminClient(ctx, cfg[models.ConfigLocation], option.WithCredentialsJSON(credential))
	if err != nil {
		return fmt.Errorf("new admin: %w", err)
	}
	defer admin.Close()

	err = admin.DeleteSubscription(ctx, subscriptionPath)
	if err != nil {
		return fmt.Errorf("delete subscription: %w", err)
	}

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

func prepareData(is *is.I, recordsCount int, cfg map[string]string) map[string]opencdc.Record {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		dest        = destination.NewDestination()
	)

	defer cancel()

	err := dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	records := make([]opencdc.Record, recordsCount)
	prepared := make(map[string]opencdc.Record, recordsCount)

	for i := 0; i < recordsCount; i++ {
		payload := uuid.NewString()

		record := opencdc.Record{
			Metadata: map[string]string{uuid.NewString(): uuid.NewString()},
			Payload:  opencdc.Change{After: opencdc.RawData(payload)},
		}

		records[i] = record
		prepared[payload] = record
	}

	n, err := dest.Write(ctx, records)
	is.NoErr(err)
	is.Equal(n, recordsCount)

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)

	return prepared
}

func compare(is *is.I, records []opencdc.Record, prepared map[string]opencdc.Record) {
	for i := range records {
		pr, ok := prepared[string(records[i].Payload.After.Bytes())]
		is.True(ok)

		is.Equal(records[i].Operation, opencdc.OperationCreate)

		createdAt, err := records[i].Metadata.GetCreatedAt()
		is.NoErr(err)
		is.True(!createdAt.IsZero())

		for k, wantMetadata := range pr.Metadata {
			if gotMetadata, ok := records[i].Metadata[k]; ok {
				// only compare fields if they actually exist
				is.Equal(wantMetadata, gotMetadata)
			}
		}

		is.Equal(records[i].Payload, pr.Payload)
	}
}
