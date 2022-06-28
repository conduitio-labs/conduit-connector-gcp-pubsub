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

package destination

import (
	"context"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/conduitio/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio/conduit-connector-gcp-pubsub/config/validator"
	"github.com/conduitio/conduit-connector-gcp-pubsub/models"
)

func TestDestination_Configure(t *testing.T) {
	dest := Destination{}

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
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
				models.ConfigClientEmail: "test@pubsub-test.iam.gserviceaccount.com",
				models.ConfigProjectID:   "pubsub-test",
				models.ConfigTopicID:     "conduit-topic-b595b388-7a97-4837-a180-380640d9c43f",
			},
			want: Destination{
				cfg: config.Destination{
					General: config.General{
						PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
						ClientEmail: "test@pubsub-test.iam.gserviceaccount.com",
						ProjectID:   "pubsub-test",
					},
					TopicID:    "conduit-topic-b595b388-7a97-4837-a180-380640d9c43f",
					BatchSize:  pubsub.DefaultPublishSettings.CountThreshold,
					BatchDelay: pubsub.DefaultPublishSettings.DelayThreshold,
				},
			},
		},
		{
			name: "valid config with all fields filled in",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
				models.ConfigClientEmail: "test@pubsub-test.iam.gserviceaccount.com",
				models.ConfigProjectID:   "pubsub-test",
				models.ConfigTopicID:     "conduit-topic-b595b388-7a97-4837-a180-380640d9c43f",
				models.ConfigBatchSize:   "10",
				models.ConfigBatchDelay:  "100ms",
			},
			want: Destination{
				cfg: config.Destination{
					General: config.General{
						PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
						ClientEmail: "test@pubsub-test.iam.gserviceaccount.com",
						ProjectID:   "pubsub-test",
					},
					TopicID:    "conduit-topic-b595b388-7a97-4837-a180-380640d9c43f",
					BatchSize:  10,
					BatchDelay: 100 * time.Millisecond,
				},
			},
		},
		{
			name: "topic id is empty",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
				models.ConfigClientEmail: "test@pubsub-test.iam.gserviceaccount.com",
				models.ConfigProjectID:   "pubsub-test",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigTopicID).Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := dest.Configure(context.Background(), tt.in)
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

			if !reflect.DeepEqual(dest.cfg, tt.want.cfg) {
				t.Errorf("parse = %v, want %v", dest.cfg, tt.want.cfg)

				return
			}
		})
	}
}