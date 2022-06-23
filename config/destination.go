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

package config

import (
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/conduitio/conduit-connector-gcp-pubsub/config/validator"
	"github.com/conduitio/conduit-connector-gcp-pubsub/models"
)

// A Destination represents a destination configuration needed for the publisher client.
type Destination struct {
	General

	// TopicID is the configuration of the topic ID for the publisher client.
	TopicID string `json:"topic_id" validate:"required,object_name"`

	// BatchSize is the configuration of the batch size for the publisher client.
	// It is the size of the batch of messages, on completing which the batch of messages will be published.
	BatchSize int `json:"batch_size" validate:"gte=1,lte=1000,omitempty"`

	// BatchDelay is the configuration of the batch delay for the publisher client.
	// It is the time delay, after which the batch of messages will be published.
	BatchDelay time.Duration `json:"batch_delay" validate:"gte=1ms,lte=1s,omitempty"`
}

// ParseDestination parses destination configuration into a configuration Destination struct.
func ParseDestination(cfg map[string]string) (Destination, error) {
	config, err := parseGeneral(cfg)
	if err != nil {
		return Destination{}, err
	}

	destinationConfig := Destination{
		General:    config,
		TopicID:    cfg[models.ConfigTopicID],
		BatchSize:  pubsub.DefaultPublishSettings.CountThreshold,
		BatchDelay: pubsub.DefaultPublishSettings.DelayThreshold,
	}

	if cfg[models.ConfigBatchSize] != "" {
		batchSize, err := strconv.Atoi(cfg[models.ConfigBatchSize])
		if err != nil {
			return Destination{}, validator.InvalidIntegerTypeErr(models.ConfigBatchSize)
		}

		destinationConfig.BatchSize = batchSize
	}

	if cfg[models.ConfigBatchDelay] != "" {
		batchDelay, err := time.ParseDuration(cfg[models.ConfigBatchDelay])
		if err != nil {
			return Destination{}, validator.InvalidTimeDurationTypeErr(models.ConfigBatchDelay)
		}

		destinationConfig.BatchDelay = batchDelay
	}

	err = validator.Validate(destinationConfig)
	if err != nil {
		return Destination{}, err
	}

	return destinationConfig, nil
}
