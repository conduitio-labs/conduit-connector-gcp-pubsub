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

	"github.com/conduitio/conduit-connector-gcp-pubsub/client"
	"github.com/conduitio/conduit-connector-gcp-pubsub/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// A Publisher represents a publisher interface.
type Publisher interface {
	Publish(context.Context, sdk.Record, sdk.AckFunc)
	Stop() error
}

// A Destination represents the destination connector.
type Destination struct {
	sdk.UnimplementedDestination
	cfg       config.Destination
	publisher Publisher
}

// New initialises a new Destination.
func New() sdk.Destination {
	return &Destination{}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (d *Destination) Configure(_ context.Context, cfgRaw map[string]string) error {
	cfg, err := config.ParseDestination(cfgRaw)
	if err != nil {
		return err
	}

	d.cfg = cfg

	return nil
}

// Open initializes a publisher client.
func (d *Destination) Open(ctx context.Context) error {
	publisher, err := client.NewPublisher(ctx, d.cfg)
	if err != nil {
		return err
	}

	d.publisher = publisher

	return nil
}

// WriteAsync writes a record into a Destination.
func (d *Destination) WriteAsync(ctx context.Context, record sdk.Record, ackFunc sdk.AckFunc) error {
	d.publisher.Publish(ctx, record, ackFunc)

	return nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("closing the connection to the GCP API service...")

	return d.publisher.Stop()
}
