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

//go:generate mockgen -typed -source=source.go -destination=mock/source.go -package=mock -mock_names=subscriber=MockSubscriber . subscriber

package source

import (
	"context"
	"errors"
	"time"

	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/clients"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jpillora/backoff"
)

// A subscriber represents a subscriber interface.
type subscriber interface {
	Next(ctx context.Context) (opencdc.Record, error)
	Ack(context.Context) error
	Stop() error
}

// A Source represents the source connector.
type Source struct {
	sdk.UnimplementedSource
	cfg        config.Source
	subscriber subscriber
}

// NewSource initialises a new source.
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() cconfig.Parameters {
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
		models.ConfigSubscriptionID: {
			Default:     "",
			Description: "GCP Pub/Sub subscription id key.",
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

// Configure parses, validates, and stores configurations.
func (s *Source) Configure(_ context.Context, cfgRaw cconfig.Config) error {
	cfg, err := config.ParseSource(cfgRaw)
	if err != nil {
		return err
	}

	s.cfg = cfg

	return nil
}

// Open initializes a subscriber client.
func (s *Source) Open(ctx context.Context, _ opencdc.Position) (err error) {
	if s.cfg.Location == "" {
		s.subscriber, err = clients.NewSubscriber(ctx, s.cfg)
		if err != nil {
			return err
		}

		return nil
	}

	s.subscriber, err = clients.NewSubscriberLite(ctx, s.cfg)
	if err != nil {
		return err
	}

	return nil
}

// Read returns the next opencdc.Record.
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	record, err := s.subscriber.Next(ctx)
	if err != nil {
		return opencdc.Record{}, err
	}

	return record, nil
}

// Ack indicates successful processing of a message passed.
func (s *Source) Ack(ctx context.Context, _ opencdc.Position) error {
	sdk.Logger(ctx).Debug().Msg("got ack")

	return s.subscriber.Ack(ctx)
}

// Teardown releases the subscriber client.
func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("closing the connection to the GCP API service...")

	if s.subscriber != nil {
		return s.subscriber.Stop()
	}

	return nil
}

// ReadWithBackoffRetry calls the Read function with a delay between calls
// until a record is returned.
func ReadWithBackoffRetry(ctx context.Context, src sdk.Source) (opencdc.Record, error) {
	b := &backoff.Backoff{
		Factor: 2,
		Min:    time.Millisecond * 100,
		Max:    time.Second,
	}

	for {
		got, err := src.Read(ctx)

		if errors.Is(err, sdk.ErrBackoffRetry) {
			select {
			case <-ctx.Done():
				return opencdc.Record{}, ctx.Err()
			case <-time.After(b.Duration()):
				continue
			}
		}

		return got, err
	}
}
