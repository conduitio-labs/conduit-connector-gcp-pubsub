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
	// ConfigPrivateKey is the configuration name of the private key for the GCP Pub/Sub client.
	ConfigPrivateKey = "privateKey"

	// ConfigClientEmail is the configuration name of the client's email for the GCP Pub/Sub client.
	ConfigClientEmail = "clientEmail"

	// ConfigProjectID is the configuration name of the project id for the GCP Pub/Sub client.
	ConfigProjectID = "projectId"

	// ConfigSubscriptionID is the configuration name of the subscription id for the subscriber client.
	ConfigSubscriptionID = "subscriptionId"

	// ConfigTopicID is the configuration name of the topic id for the publisher client.
	ConfigTopicID = "topicId"

	// ConfigBatchSize is the configuration name of the batch size for the publisher client.
	ConfigBatchSize = "batchSize"

	// ConfigBatchDelay is the configuration name of the batch delay for the publisher client.
	ConfigBatchDelay = "batchDelay"

	// ConfigLocation is the configuration name of the Cloud Region or Zone where the topic resides.
	ConfigLocation = "location"
)

// ConfigKeyName returns a configuration key name by struct field.
func ConfigKeyName(fieldName string) string {
	return map[string]string{
		"PrivateKey":     ConfigPrivateKey,
		"ClientEmail":    ConfigClientEmail,
		"ProjectID":      ConfigProjectID,
		"Location":       ConfigLocation,
		"SubscriptionID": ConfigSubscriptionID,
		"TopicID":        ConfigTopicID,
		"BatchSize":      ConfigBatchSize,
		"BatchDelay":     ConfigBatchDelay,
	}[fieldName]
}
