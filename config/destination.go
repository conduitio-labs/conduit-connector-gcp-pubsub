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
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config/validator"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
)

// A Destination represents a destination configuration needed for the publisher client.
type Destination struct {
	General

	// TopicID is the configuration of the topic id for the publisher client.
	TopicID string `validate:"required,object_name"`
}

// ParseDestination parses destination configuration into a configuration Destination struct.
func ParseDestination(cfg map[string]string) (Destination, error) {
	config, err := parseGeneral(cfg)
	if err != nil {
		return Destination{}, err
	}

	destinationConfig := Destination{
		General: config,
		TopicID: cfg[models.ConfigTopicID],
	}

	err = validator.Validate(destinationConfig)
	if err != nil {
		return Destination{}, err
	}

	return destinationConfig, nil
}
