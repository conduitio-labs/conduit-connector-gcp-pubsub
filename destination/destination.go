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

//go:generate mockgen -typed -source=destination.go -destination=mock/destination.go -package=mock -mock_names=publisher=MockPublisher . publisher

package destination

import (
	"context"

	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/clients"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// A publisher represents a publisher interface.
type publisher interface {
	Publish(context.Context, opencdc.Record) error
	Stop() error
}

// A Destination represents the destination connector.
type Destination struct {
	sdk.UnimplementedDestination
	cfg       config.Destination
	publisher publisher
}

// NewDestination initialises a new Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (d *Destination) Parameters() cconfig.Parameters {
	return map[string]cconfig.Parameter{
		models.ConfigPrivateKey: {
			Default:     "",
			Description: "GCP Pub/Sub private key.",
			Validations: []cconfig.Validation{
				cconfig.ValidationRequired{},
			},
		},
		models.ConfigClientEmail: {
			Default:     "",
			Description: "GCP Pub/Sub client email key.",
			Validations: []cconfig.Validation{
				cconfig.ValidationRequired{},
			},
		},
		models.ConfigProjectID: {
			Default:     "",
			Description: "GCP Pub/Sub project id key.",
			Validations: []cconfig.Validation{
				cconfig.ValidationRequired{},
			},
		},
		models.ConfigTopicID: {
			Default:     "",
			Description: "GCP Pub/Sub topic id key.",
			Validations: []cconfig.Validation{
				cconfig.ValidationRequired{},
			},
		},
		models.ConfigLocation: {
			Default:     "",
			Description: "Cloud Region or Zone where the topic resides (for GCP Pub/Sub Lite only).",
			Validations: []cconfig.Validation{
				cconfig.ValidationRequired{},
			},
		},
	}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (d *Destination) Configure(_ context.Context, cfgRaw cconfig.Config) error {
	cfg, err := config.ParseDestination(cfgRaw)
	if err != nil {
		return err
	}

	d.cfg = cfg

	return nil
}

// Open initializes a publisher client.
func (d *Destination) Open(ctx context.Context) (err error) {
	if d.cfg.Location == "" {
		d.publisher, err = clients.NewPublisher(ctx, d.cfg)
		if err != nil {
			return err
		}

		return nil
	}

	d.publisher, err = clients.NewPublisherLite(ctx, d.cfg)
	if err != nil {
		return err
	}

	return nil
}

// Write writes records into a Destination.
func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	for i := range records {
		err := d.publisher.Publish(ctx, records[i])
		if err != nil {
			return i, err
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("closing the connection to the GCP API service...")

	if d.publisher != nil {
		return d.publisher.Stop()
	}

	return nil
}
