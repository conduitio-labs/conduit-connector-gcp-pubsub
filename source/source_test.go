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
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config/validator"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/source/mock"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

func TestSource_Configure(t *testing.T) {
	src := Source{}

	tests := []struct {
		name        string
		in          map[string]string
		want        Source
		expectedErr error
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
			expectedErr: validator.RequiredErr(models.ConfigPrivateKey),
		},
		{
			name: "client email is empty",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
				models.ConfigProjectID:      "pubsub-test",
				models.ConfigSubscriptionID: "conduit-subscription-b595b388-7a97-4837-a180-380640d9c43f",
			},
			expectedErr: validator.RequiredErr(models.ConfigClientEmail),
		},
		{
			name: "project id is empty",
			in: map[string]string{
				models.ConfigPrivateKey:     "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
				models.ConfigClientEmail:    "test@pubsub-test.iam.gserviceaccount.com",
				models.ConfigSubscriptionID: "conduit-subscription-b595b388-7a97-4837-a180-380640d9c43f",
			},
			expectedErr: validator.RequiredErr(models.ConfigProjectID),
		},
		{
			name: "subscription id is empty",
			in: map[string]string{
				models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
				models.ConfigClientEmail: "test@pubsub-test.iam.gserviceaccount.com",
				models.ConfigProjectID:   "pubsub-test",
			},
			expectedErr: validator.RequiredErr(models.ConfigSubscriptionID),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := src.Configure(context.Background(), tt.in)
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

			if !reflect.DeepEqual(src.cfg, tt.want.cfg) {
				t.Errorf("parse = %v, want %v", src.cfg, tt.want.cfg)

				return
			}
		})
	}
}

func TestSource_ReadSuccess(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	st := make(sdk.StructuredData)
	st["key"] = "value"

	record := sdk.Record{
		Position:  sdk.Position(`{"last_processed_element_value": 1}`),
		Metadata:  nil,
		CreatedAt: time.Time{},
		Payload:   st,
	}

	sub := mock.NewMockSubscriber(ctrl)
	sub.EXPECT().Next(ctx).Return(record, nil)

	s := Source{
		subscriber: sub,
	}

	r, err := s.Read(ctx)
	is.NoErr(err)

	is.Equal(r, record)
}

func TestSource_ReadFail(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	sub := mock.NewMockSubscriber(ctrl)
	sub.EXPECT().Next(ctx).Return(sdk.Record{}, errors.New("key is not exist"))

	s := Source{
		subscriber: sub,
	}

	_, err := s.Read(ctx)
	is.Equal(err != nil, true)
}

func TestSource_AckSuccess(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	sub := mock.NewMockSubscriber(ctrl)
	sub.EXPECT().Ack(ctx).Return(nil)

	s := Source{
		subscriber: sub,
	}

	err := s.Ack(ctx, nil)
	is.NoErr(err)
}

func TestSource_AckFail(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	sub := mock.NewMockSubscriber(ctrl)
	sub.EXPECT().Ack(ctx).Return(context.Canceled)

	s := Source{
		subscriber: sub,
	}

	err := s.Ack(ctx, nil)
	is.Equal(err, context.Canceled)
}

func TestSource_TeardownSuccess(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)

	sub := mock.NewMockSubscriber(ctrl)
	sub.EXPECT().Stop().Return(nil)

	s := Source{
		subscriber: sub,
	}

	err := s.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_TeardownFail(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)

	sub := mock.NewMockSubscriber(ctrl)
	sub.EXPECT().Stop().Return(errors.New("pubsub closing error"))

	s := Source{
		subscriber: sub,
	}

	err := s.Teardown(context.Background())
	is.Equal(err != nil, true)
}
