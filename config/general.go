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
	"github.com/conduitio/conduit-connector-gcp-pubsub/config/validator"
	"github.com/conduitio/conduit-connector-gcp-pubsub/models"
)

type general struct {
	PrivateKey  string `validate:"required"`
	ClientEmail string `validate:"required,email"`
	ProjectID   string `validate:"required"`
	TopicID     string `validate:"required,object_name"`
}

func parseGeneral(cfg map[string]string) (general, error) {
	config := general{
		PrivateKey:  cfg[models.ConfigPrivateKey],
		ClientEmail: cfg[models.ConfigClientEmail],
		ProjectID:   cfg[models.ConfigProjectID],
		TopicID:     cfg[models.ConfigTopicID],
	}

	err := validator.Validate(config)
	if err != nil {
		return general{}, err
	}

	return config, nil
}
