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
		wantErr     bool
		expectedErr string
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
			},
		},
		{
			name: "private key is validator.Required",
			in: map[string]string{
				models.ConfigClientEmail: "test@test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigPrivateKey).Error(),
		},
		{
			name: "client email is validator.Required",
			in: map[string]string{
				models.ConfigPrivateKey: "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigProjectID:  "test-pubsub",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigClientEmail).Error(),
		},
		{
			name: "project id is validator.Required",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test@test-pubsub.com",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigProjectID).Error(),
		},
		{
			name: "a couple fields are empty (a client email and a project id)",
			in: map[string]string{
				models.ConfigPrivateKey: "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
			},
			wantErr: true,
			expectedErr: multierr.Combine(validator.RequiredErr(models.ConfigClientEmail),
				validator.RequiredErr(models.ConfigProjectID)).Error(),
		},
		{
			name: "invalid email",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAQEFAASC-----END PRIVATE KEY-----",
				models.ConfigClientEmail: "test-pubsub.com",
				models.ConfigProjectID:   "test-pubsub",
			},
			wantErr:     true,
			expectedErr: validator.InvalidEmailErr(models.ConfigClientEmail).Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseGeneral(tt.in)
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
