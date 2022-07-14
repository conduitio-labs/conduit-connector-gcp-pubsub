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

package source

import (
	"context"
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config/validator"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
)

func TestSource_Configure(t *testing.T) {
	src := Source{}

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
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
				models.ConfigClientEmail:    "test@pubsub-test.iam.gserviceaccount.com",
				models.ConfigProjectID:      "pubsub-test",
				models.ConfigSubscriptionID: "conduit-subscription-b595b388-7a97-4837-a180-380640d9c43f",
			},
			want: Source{
				cfg: config.Source{
					General: config.General{
						PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
						ClientEmail: "test@pubsub-test.iam.gserviceaccount.com",
						ProjectID:   "pubsub-test",
					},
					SubscriptionID: "conduit-subscription-b595b388-7a97-4837-a180-380640d9c43f",
				},
			},
		},
		{
			name: "private key is empty",
			in: map[string]string{
				models.ConfigClientEmail:    "test@pubsub-test.iam.gserviceaccount.com",
				models.ConfigProjectID:      "pubsub-test",
				models.ConfigSubscriptionID: "conduit-subscription-b595b388-7a97-4837-a180-380640d9c43f",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigPrivateKey).Error(),
		},
		{
			name: "client email is empty",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
				models.ConfigProjectID:      "pubsub-test",
				models.ConfigSubscriptionID: "conduit-subscription-b595b388-7a97-4837-a180-380640d9c43f",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigClientEmail).Error(),
		},
		{
			name: "project id is empty",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
				models.ConfigClientEmail:    "test@pubsub-test.iam.gserviceaccount.com",
				models.ConfigSubscriptionID: "conduit-subscription-b595b388-7a97-4837-a180-380640d9c43f",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigProjectID).Error(),
		},
		{
			name: "subscription id is empty",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
				models.ConfigClientEmail: "test@pubsub-test.iam.gserviceaccount.com",
				models.ConfigProjectID:   "pubsub-test",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigSubscriptionID).Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := src.Configure(context.Background(), tt.in)
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

			if !reflect.DeepEqual(src.cfg, tt.want.cfg) {
				t.Errorf("parse = %v, want %v", src.cfg, tt.want.cfg)

				return
			}
		})
	}
}
