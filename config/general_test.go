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
	"go.uber.org/multierr"
)

func TestParseGeneral(t *testing.T) {
	tests := []struct {
		name        string
		in          map[string]string
		want        General
		expectedErr error
	}{
		{
			name: "valid config",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
			},
			want: General{
				PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				ClientEmail: "test@test-pubsub.com",
				ProjectID:   "test-pubsub",
				Location:    "",
			},
		},
		{
			name: "location is not empty",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
				models.ConfigLocation:    "europe-central2-a",
			},
			want: General{
				PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				ClientEmail: "test@test-pubsub.com",
				ProjectID:   "test-pubsub",
				Location:    "europe-central2-a",
			},
		},
		{
			name: "private key is required",
			in: map[string]string{
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
			},
			expectedErr: validator.RequiredErr(models.ConfigPrivateKey),
		},
		{
			name: "client email is required",
			in: map[string]string{
				models.ConfigPrivateKey: "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigProjectID:  "test-pubsub",
			},
			expectedErr: validator.RequiredErr(models.ConfigClientEmail),
		},
		{
			name: "project id is required",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
			},
			expectedErr: validator.RequiredErr(models.ConfigProjectID),
		},
		{
			name: "a couple fields are empty (a client email and a project id)",
			in: map[string]string{
				models.ConfigPrivateKey: "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
			},
			expectedErr: multierr.Combine(validator.RequiredErr(models.ConfigClientEmail),
				validator.RequiredErr(models.ConfigProjectID)),
		},
		{
			name: "invalid email",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
			},
			expectedErr: validator.InvalidEmailErr(models.ConfigClientEmail),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseGeneral(tt.in)
			if err != nil {
				if tt.expectedErr == nil {
					t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tt.expectedErr != nil)

					return
				}

				if err.Error() != tt.expectedErr.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", tt.expectedErr.Error(), err.Error())

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
