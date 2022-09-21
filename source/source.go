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
	"time"

	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/clients"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jpillora/backoff"
)

// A subscriber represents a subscriber interface.
type subscriber interface {
	Next(ctx context.Context) (sdk.Record, error)
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
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		models.ConfigPrivateKey: {
			Default:     "",
			Required:    true,
			Description: "GCP Pub/Sub private key.",
		},
		models.ConfigClientEmail: {
			Default:     "",
			Required:    true,
			Description: "GCP Pub/Sub client email key.",
		},
		models.ConfigProjectID: {
			Default:     "",
			Required:    true,
			Description: "GCP Pub/Sub project id key.",
		},
		models.ConfigSubscriptionID: {
			Default:     "",
			Required:    true,
			Description: "GCP Pub/Sub subscription id key.",
		},
		models.ConfigLocation: {
			Default:     "",
			Required:    false,
			Description: "Cloud Region or Zone where the topic resides (for GCP Pub/Sub Lite only).",
		},
	}
}

// Configure parses, validates, and stores configurations.
func (s *Source) Configure(_ context.Context, cfgRaw map[string]string) error {
	cfg, err := config.ParseSource(cfgRaw)
	if err != nil {
		return err
	}

	s.cfg = cfg

	return nil
}

// Open initializes a subscriber client.
func (s *Source) Open(ctx context.Context, _ sdk.Position) (err error) {
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

// Read returns the next sdk.Record.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	record, err := s.subscriber.Next(ctx)
	if err != nil {
		return sdk.Record{}, err
	}

	return record, nil
}

// Ack indicates successful processing of a message passed.
func (s *Source) Ack(ctx context.Context, _ sdk.Position) error {
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
func ReadWithBackoffRetry(ctx context.Context, src sdk.Source) (sdk.Record, error) {
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
				return sdk.Record{}, ctx.Err()
			case <-time.After(b.Duration()):
				continue
			}
		}

		return got, err
	}
}
