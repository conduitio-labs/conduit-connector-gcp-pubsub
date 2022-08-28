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
	"errors"
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config/validator"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/destination/mock"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

func TestDestination_Configure(t *testing.T) {
	t.Parallel()

	dest := Destination{}

	tests := []struct {
		name string
		in   map[string]string
		want Destination
		err  error
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
					TopicID: "conduit-topic-b595b388-7a97-4837-a180-380640d9c43f",
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
			err: validator.RequiredErr(models.ConfigTopicID),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := dest.Configure(context.Background(), tt.in)
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

			if !reflect.DeepEqual(dest.cfg, tt.want.cfg) {
				t.Errorf("parse = %v, want %v", dest.cfg, tt.want.cfg)

				return
			}
		})
	}
}

func TestDestination_TeardownSuccess(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)

	pub := mock.NewMockPublisher(ctrl)
	pub.EXPECT().Stop().Return(nil)

	d := Destination{
		publisher: pub,
	}

	err := d.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_TeardownFail(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)

	pub := mock.NewMockPublisher(ctrl)
	pub.EXPECT().Stop().Return(errors.New("pubsub closing error"))

	d := Destination{
		publisher: pub,
	}

	err := d.Teardown(context.Background())
	is.Equal(err != nil, true)
}
