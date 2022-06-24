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

package gcppubsub

import (
	"strconv"

	"cloud.google.com/go/pubsub"
	"github.com/conduitio/conduit-connector-gcp-pubsub/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Specification returns specification of the connector.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "gcp-pub-sub",
		Summary: "A GCP Pub/Sub source and destination plugin for Conduit, written in Go.",
		Description: "The Google Cloud Platform Pub/Sub connector is one of Conduit plugins. " +
			"It provides a source and a destination GCP Pub/Sub connector.",
		Version: "v0.1.0",
		Author:  "Meroxa, Inc.",
		SourceParams: map[string]sdk.Parameter{
			models.ConfigPrivateKey: {
				Default:     "",
				Required:    true,
				Description: "GCP Pub/Sub private key.",
			},
			models.ConfigClientEmail: {
				Default:     "",
				Required:    true,
				Description: "GCP Pub/Sub client email key.",
			},
			models.ConfigProjectID: {
				Default:     "",
				Required:    true,
				Description: "GCP Pub/Sub project id key.",
			},
			models.ConfigSubscriptionID: {
				Default:     "",
				Required:    true,
				Description: "GCP Pub/Sub subscription id key.",
			},
		},
		DestinationParams: map[string]sdk.Parameter{
			models.ConfigPrivateKey: {
				Default:     "",
				Required:    true,
				Description: "GCP Pub/Sub private key.",
			},
			models.ConfigClientEmail: {
				Default:     "",
				Required:    true,
				Description: "GCP Pub/Sub client email key.",
			},
			models.ConfigProjectID: {
				Default:     "",
				Required:    true,
				Description: "GCP Pub/Sub project id key.",
			},
			models.ConfigTopicID: {
				Default:     "",
				Required:    true,
				Description: "GCP Pub/Sub topic id key.",
			},
			models.ConfigBatchSize: {
				Default:     strconv.Itoa(pubsub.DefaultPublishSettings.CountThreshold),
				Required:    false,
				Description: "GCP Pub/Sub batch size key.",
			},
			models.ConfigBatchDelay: {
				Default:     pubsub.DefaultPublishSettings.DelayThreshold.String(),
				Required:    false,
				Description: "GCP Pub/Sub batch delay key.",
			},
		},
	}
}
