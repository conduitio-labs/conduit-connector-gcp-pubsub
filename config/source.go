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

// A Source represents a source configuration needed for the subscriber client.
type Source struct {
	General

	// SubscriptionID is the configuration of the subscription id for the subscriber client.
	SubscriptionID string `validate:"required,object_name"`
}

// ParseSource parses source configuration into a configuration Source struct.
func ParseSource(cfg map[string]string) (Source, error) {
	config, err := parseGeneral(cfg)
	if err != nil {
		return Source{}, err
	}

	sourceConfig := Source{
		General:        config,
		SubscriptionID: cfg[models.ConfigSubscriptionID],
	}

	err = validator.Validate(sourceConfig)
	if err != nil {
		return Source{}, err
	}

	return sourceConfig, nil
}
