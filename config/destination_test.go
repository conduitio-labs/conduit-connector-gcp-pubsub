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

	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config/validator"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
)

func TestParseDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]string
		want Destination
		err  error
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
				TopicID: "test-TOPic.scription~%_1230+",
			},
		},
		{
			name: "topic id is too short",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "to",
			},
			err: validator.InvalidNameErr(models.ConfigTopicID),
		},
		{
			name: "topic id is too long",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID: "test-KfAvXlueAOAL5UhJ1Rr4zzsZJnB1ilOYXrxgjUmJqD1TZdEC8NAl22oPrzx1HpB7MfgteQUMn8u" +
					"NCadTgrbUJKY6Lb6ARzyOY3bI3W6YjadDLTl47DIqA7zjYYQNIud9PHXgA0v3NVlk2AVLaziUwylawemiUJOee68ULPg" +
					"GyBgoCIMAB7ukAgjN0fhGPeYART2yojioOp3w9mBPdklk7OY8rJQFCy5ii70byyHFqT3JG00kJTPzdPPdt53",
			},
			err: validator.InvalidNameErr(models.ConfigTopicID),
		},
		{
			name: "topic id does not start with a letter",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "1-test-top*",
			},
			err: validator.InvalidNameErr(models.ConfigTopicID),
		},
		{
			name: "topic id has unsupported characters",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "test-top*",
			},
			err: validator.InvalidNameErr(models.ConfigTopicID),
		},
		{
			name: "topic id starts with goog",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "goog-test-topic",
			},
			err: validator.InvalidNameErr(models.ConfigTopicID),
		},
		{
			name: "topic id starts with Goog",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "Goog-test-topic",
			},
			err: validator.InvalidNameErr(models.ConfigTopicID),
		},
		{
			name: "topic id starts with gooG",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigTopicID:     "gooG-test-topic",
			},
			err: validator.InvalidNameErr(models.ConfigTopicID),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseDestination(tt.in)
			if err != nil {
				if tt.err == nil {
					t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tt.err != nil)

					return
				}

				if err.Error() != tt.err.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", tt.err.Error(), err.Error())

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
