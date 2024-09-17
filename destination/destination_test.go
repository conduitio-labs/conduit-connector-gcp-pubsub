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
	"testing"

	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config/validator"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/destination/mock"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

func TestDestination_ConfigureSuccess(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := Destination{}

	err := d.Configure(context.Background(), map[string]string{
		models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
		models.ConfigClientEmail: "test@pubsub-test.iam.gserviceaccount.com",
		models.ConfigProjectID:   "pubsub-test",
		models.ConfigTopicID:     "conduit-topic-b595b388-7a97-4837-a180-380640d9c43f",
	})
	is.NoErr(err)
	is.Equal(d.cfg, config.Destination{
		General: config.General{
			PrivateKey:  "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
			ClientEmail: "test@pubsub-test.iam.gserviceaccount.com",
			ProjectID:   "pubsub-test",
		},
		TopicID: "conduit-topic-b595b388-7a97-4837-a180-380640d9c43f",
	})
}

func TestDestination_ConfigureFail(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := Destination{}

	err := d.Configure(context.Background(), map[string]string{
		models.ConfigPrivateKey:  "-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n",
		models.ConfigClientEmail: "test@pubsub-test.iam.gserviceaccount.com",
		models.ConfigProjectID:   "pubsub-test",
	})
	is.Equal(err, validator.RequiredErr(models.ConfigTopicID))
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
	is.Equal(err, errors.New("pubsub closing error"))
}
