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
	"encoding/json"
	"fmt"

	"github.com/conduitio/conduit-connector-gcp-pubsub/config/validator"
	"github.com/conduitio/conduit-connector-gcp-pubsub/models"
)

// A General represents a general configuration needed for GCP Pub/Sub.
type General struct {
	// PrivateKey is the configuration name for GCP Pub/Sub client private key.
	PrivateKey string `json:"private_key" validate:"required"`

	// ClientEmail is the configuration name for GCP Pub/Sub client email.
	ClientEmail string `json:"client_email" validate:"required,email"`

	// ProjectID is the configuration name for GCP Pub/Sub client project id.
	ProjectID string `json:"project_id" validate:"required"`
}

func parseGeneral(cfg map[string]string) (General, error) {
	config := General{
		PrivateKey:  cfg[models.ConfigPrivateKey],
		ClientEmail: cfg[models.ConfigClientEmail],
		ProjectID:   cfg[models.ConfigProjectID],
	}

	err := validator.Validate(config)
	if err != nil {
		return General{}, err
	}

	return config, nil
}

// Marshal converts General configuration into a binary representation.
func (cfg General) Marshal() ([]byte, error) {
	const credentialType = "service_account"

	credentialStruct := struct {
		General
		Type string `json:"type"`
	}{
		General: cfg,
		Type:    credentialType,
	}

	credential, err := json.Marshal(credentialStruct)
	if err != nil {
		return nil, fmt.Errorf("marshal creadential: %w", err)
	}

	return credential, nil
}
