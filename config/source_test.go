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
	"reflect"
	"testing"

	"github.com/conduitio/conduit-connector-gcp-pubsub/config/validator"
	"github.com/conduitio/conduit-connector-gcp-pubsub/models"
)

func TestParseSource(t *testing.T) {
	tests := []struct {
		name        string
		in          map[string]string
		want        Source
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid config",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail:    "test@test-pubsub.com",
				models.ConfigProjectID:      "test-pubsub",
				models.ConfigTopicID:        "test-T.o~pic_123+%",
				models.ConfigSubscriptionID: "test-SUb.scription~%_1230+",
				models.ConfigEnableOrdering: "true",
			},
			want: Source{
				general: general{
					PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
					ClientEmail: "test@test-pubsub.com",
					ProjectID:   "test-pubsub",
					TopicID:     "test-T.o~pic_123+%",
				},
				SubscriptionID: "test-SUb.scription~%_1230+",
				EnableOrdering: true,
			},
		},
		{
			name: "subscription id is too small",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail:    "test@test-pubsub.com",
				models.ConfigProjectID:      "test-pubsub",
				models.ConfigTopicID:        "test-topic",
				models.ConfigSubscriptionID: "su",
			},
			wantErr:     true,
			expectedErr: validator.InvalidNameErr(models.ConfigSubscriptionID).Error(),
		},
		{
			name: "subscription id is too big",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigSubscriptionID: "test-KfAvXlueAOAL5UhJ1Rr4zzsZJnB1ilOYXrxgjUmJqD1TZdEC8NAl22oPrzx1HpB7MfgteQUMn8u" +
					"NCadTgrbUJKY6Lb6ARzyOY3bI3W6YjadDLTl47DIqA7zjYYQNIud9PHXgA0v3NVlk2AVLaziUwylawemiUJOee68ULPg" +
					"GyBgoCIMAB7ukAgjN0fhGPeYART2yojioOp3w9mBPdklk7OY8rJQFCy5ii70byyHFqT3JG00kJTPzdPPdt53",
			},
			wantErr:     true,
			expectedErr: validator.InvalidNameErr(models.ConfigSubscriptionID).Error(),
		},
		{
			name: "subscription id has unsupported characters",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail:    "test@test-pubsub.com",
				models.ConfigProjectID:      "test-pubsub",
				models.ConfigTopicID:        "test-topic",
				models.ConfigSubscriptionID: "test-sub*",
			},
			wantErr:     true,
			expectedErr: validator.InvalidNameErr(models.ConfigSubscriptionID).Error(),
		},
		{
			name: "subscription id starts with goog",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail:    "test@test-pubsub.com",
				models.ConfigProjectID:      "test-pubsub",
				models.ConfigTopicID:        "test-topic",
				models.ConfigSubscriptionID: "goog-test-subscription",
			},
			wantErr:     true,
			expectedErr: validator.InvalidNameErr(models.ConfigSubscriptionID).Error(),
		},
		{
			name: "enable ordering is not a bool",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail:    "test@test-pubsub.com",
				models.ConfigProjectID:      "test-pubsub",
				models.ConfigTopicID:        "test-topic",
				models.ConfigSubscriptionID: "test-subscription",
				models.ConfigEnableOrdering: "validator.Invalid-bool",
			},
			wantErr:     true,
			expectedErr: validator.InvalidBoolErr(models.ConfigEnableOrdering).Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSource(tt.in)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tt.wantErr)

					return
				}

				if err.Error() != tt.expectedErr {
					t.Errorf("expected error \"%s\", got \"%s\"", tt.expectedErr, err.Error())

					return
				}

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parse = %v, want %v", got, tt.want)
			}
		})
	}
}
