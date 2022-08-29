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

func TestParseSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]string
		want Source
		err  error
	}{
		{
			name: "valid config",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail:    "test@test-pubsub.com",
				models.ConfigProjectID:      "test-pubsub",
				models.ConfigSubscriptionID: "test-SUb.scription~%_1230+",
			},
			want: Source{
				General: General{
					PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
					ClientEmail: "test@test-pubsub.com",
					ProjectID:   "test-pubsub",
				},
				SubscriptionID: "test-SUb.scription~%_1230+",
			},
		},
		{
			name: "subscription id is too small",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail:    "test@test-pubsub.com",
				models.ConfigProjectID:      "test-pubsub",
				models.ConfigSubscriptionID: "su",
			},
			err: validator.InvalidNameErr(models.ConfigSubscriptionID),
		},
		{
			name: "subscription id is too big",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigSubscriptionID: "test-KfAvXlueAOAL5UhJ1Rr4zzsZJnB1ilOYXrxgjUmJqD1TZdEC8NAl22oPrzx1HpB7MfgteQUMn8u" +
					"NCadTgrbUJKY6Lb6ARzyOY3bI3W6YjadDLTl47DIqA7zjYYQNIud9PHXgA0v3NVlk2AVLaziUwylawemiUJOee68ULPg" +
					"GyBgoCIMAB7ukAgjN0fhGPeYART2yojioOp3w9mBPdklk7OY8rJQFCy5ii70byyHFqT3JG00kJTPzdPPdt53",
			},
			err: validator.InvalidNameErr(models.ConfigSubscriptionID),
		},
		{
			name: "subscription id does not start with a letter",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail:    "test@test-pubsub.com",
				models.ConfigProjectID:      "test-pubsub",
				models.ConfigSubscriptionID: "1-test-subscription",
			},
			err: validator.InvalidNameErr(models.ConfigSubscriptionID),
		},
		{
			name: "subscription id has unsupported characters",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail:    "test@test-pubsub.com",
				models.ConfigProjectID:      "test-pubsub",
				models.ConfigSubscriptionID: "test-sub*",
			},
			err: validator.InvalidNameErr(models.ConfigSubscriptionID),
		},
		{
			name: "subscription id starts with goog",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail:    "test@test-pubsub.com",
				models.ConfigProjectID:      "test-pubsub",
				models.ConfigSubscriptionID: "goog-test-subscription",
			},
			err: validator.InvalidNameErr(models.ConfigSubscriptionID),
		},
		{
			name: "subscription id starts with Goog",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail:    "test@test-pubsub.com",
				models.ConfigProjectID:      "test-pubsub",
				models.ConfigSubscriptionID: "Goog-test-subscription",
			},
			err: validator.InvalidNameErr(models.ConfigSubscriptionID),
		},
		{
			name: "subscription id starts with gooG",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail:    "test@test-pubsub.com",
				models.ConfigProjectID:      "test-pubsub",
				models.ConfigSubscriptionID: "gooG-test-subscription",
			},
			err: validator.InvalidNameErr(models.ConfigSubscriptionID),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseSource(tt.in)
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
