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

package models

const (
	// ConfigPrivateKey is the configuration name for GCP Pub/Sub private key.
	ConfigPrivateKey = "privateKey"

	// ConfigClientEmail is the configuration name for GCP Pub/Sub client's email.
	ConfigClientEmail = "clientEmail"

	// ConfigProjectID is the configuration name for GCP Pub/Sub project ID.
	ConfigProjectID = "projectID"

	// ConfigSubscriptionID is the configuration name for GCP Pub/Sub subscription ID.
	ConfigSubscriptionID = "subscriptionID"

	// ConfigBatchSize is the configuration name for GCP Pub/Sub batch size.
	ConfigBatchSize = "batchSize"

	// ConfigBatchDelay is the configuration name for GCP Pub/Sub batch delay.
	ConfigBatchDelay = "batchDelay"
)

// ConfigKeyName returns a configuration key name by struct field.
func ConfigKeyName(fieldName string) string {
	return map[string]string{
		"PrivateKey":     ConfigPrivateKey,
		"ClientEmail":    ConfigClientEmail,
		"ProjectID":      ConfigProjectID,
		"SubscriptionID": ConfigSubscriptionID,
		"BatchSize":      ConfigBatchSize,
		"BatchDelay":     ConfigBatchDelay,
	}[fieldName]
}
