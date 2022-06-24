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
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/conduitio/conduit-connector-gcp-pubsub/config/validator"
	"github.com/conduitio/conduit-connector-gcp-pubsub/models"
)

func TestParseDestination(t *testing.T) {
	tests := []struct {
		name        string
		in          map[string]string
		want        Destination
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid config with only required fields",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-TOPic.scription~%_1230+",
			},
			want: Destination{
				General: General{
					PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
					ClientEmail: "test@test-pubsub.com",
					ProjectID:   "test-pubsub",
				},
				TopicID:    "test-TOPic.scription~%_1230+",
				BatchSize:  pubsub.DefaultPublishSettings.CountThreshold,
				BatchDelay: pubsub.DefaultPublishSettings.DelayThreshold,
			},
		},
		{
			name: "valid config with all fields filled in",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchSize:   "10",
				models.ConfigBatchDelay:  "100ms",
			},
			want: Destination{
				General: General{
					PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
					ClientEmail: "test@test-pubsub.com",
					ProjectID:   "test-pubsub",
				},
				TopicID:    "test-topic",
				BatchSize:  10,
				BatchDelay: 100 * time.Millisecond,
			},
		},
		{
			name: "topic id is too small",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "to",
			},
			wantErr:     true,
			expectedErr: validator.InvalidNameErr(models.ConfigTopicID).Error(),
		},
		{
			name: "topic id is too big",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID: "test-KfAvXlueAOAL5UhJ1Rr4zzsZJnB1ilOYXrxgjUmJqD1TZdEC8NAl22oPrzx1HpB7MfgteQUMn8u" +
					"NCadTgrbUJKY6Lb6ARzyOY3bI3W6YjadDLTl47DIqA7zjYYQNIud9PHXgA0v3NVlk2AVLaziUwylawemiUJOee68ULPg" +
					"GyBgoCIMAB7ukAgjN0fhGPeYART2yojioOp3w9mBPdklk7OY8rJQFCy5ii70byyHFqT3JG00kJTPzdPPdt53",
			},
			wantErr:     true,
			expectedErr: validator.InvalidNameErr(models.ConfigTopicID).Error(),
		},
		{
			name: "topic id has unsupported characters",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-top*",
			},
			wantErr:     true,
			expectedErr: validator.InvalidNameErr(models.ConfigTopicID).Error(),
		},
		{
			name: "topic id starts with goog",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "goog-test-topic",
			},
			wantErr:     true,
			expectedErr: validator.InvalidNameErr(models.ConfigTopicID).Error(),
		},
		{
			name: "batch size is maximum",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchSize:   "1000",
			},
			want: Destination{
				General: General{
					PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
					ClientEmail: "test@test-pubsub.com",
					ProjectID:   "test-pubsub",
				},
				TopicID:    "test-topic",
				BatchSize:  1000,
				BatchDelay: pubsub.DefaultPublishSettings.DelayThreshold,
			},
		},
		{
			name: "batch size is minimum",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchSize:   "1",
			},
			want: Destination{
				General: General{
					PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
					ClientEmail: "test@test-pubsub.com",
					ProjectID:   "test-pubsub",
				},
				TopicID:    "test-topic",
				BatchSize:  1,
				BatchDelay: pubsub.DefaultPublishSettings.DelayThreshold,
			},
		},
		{
			name: "batch delay is maximum",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchDelay:  "1s",
			},
			want: Destination{
				General: General{
					PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
					ClientEmail: "test@test-pubsub.com",
					ProjectID:   "test-pubsub",
				},
				TopicID:    "test-topic",
				BatchSize:  pubsub.DefaultPublishSettings.CountThreshold,
				BatchDelay: 1 * time.Second,
			},
		},
		{
			name: "batch delay is minimum",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchDelay:  "1ms",
			},
			want: Destination{
				General: General{
					PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
					ClientEmail: "test@test-pubsub.com",
					ProjectID:   "test-pubsub",
				},
				TopicID:    "test-topic",
				BatchSize:  pubsub.DefaultPublishSettings.CountThreshold,
				BatchDelay: 1 * time.Millisecond,
			},
		},
		{
			name: "batch size with a wrong data type",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchSize:   "test",
			},
			wantErr:     true,
			expectedErr: validator.InvalidIntegerTypeErr(models.ConfigBatchSize).Error(),
		},
		{
			name: "batch size is zero",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchSize:   "0",
			},
			wantErr:     true,
			expectedErr: validator.OutOfRangeErr(models.ConfigBatchSize).Error(),
		},
		{
			name: "batch size is negative",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchSize:   "-1",
			},
			wantErr:     true,
			expectedErr: validator.OutOfRangeErr(models.ConfigBatchSize).Error(),
		},
		{
			name: "batch size is too big",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchSize:   "1001",
			},
			wantErr:     true,
			expectedErr: validator.OutOfRangeErr(models.ConfigBatchSize).Error(),
		},
		{
			name: "batch delay with a wrong data type",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchDelay:  "test",
			},
			wantErr:     true,
			expectedErr: validator.InvalidTimeDurationTypeErr(models.ConfigBatchDelay).Error(),
		},
		{
			name: "batch delay is zero",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchDelay:  "0",
			},
			wantErr:     true,
			expectedErr: validator.OutOfRangeErr(models.ConfigBatchDelay).Error(),
		},
		{
			name: "batch delay is less than the minimum",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchDelay:  "100ns",
			},
			wantErr:     true,
			expectedErr: validator.OutOfRangeErr(models.ConfigBatchDelay).Error(),
		},
		{
			name: "batch delay is too big",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-topic",
				models.ConfigBatchDelay:  "2s",
			},
			wantErr:     true,
			expectedErr: validator.OutOfRangeErr(models.ConfigBatchDelay).Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDestination(tt.in)
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