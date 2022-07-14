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

	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/client"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// A Subscriber represents a subscriber interface.
type Subscriber interface {
	Next(ctx context.Context) (sdk.Record, error)
	Ack(context.Context) error
	Stop() error
}

// A Source represents the source connector.
type Source struct {
	sdk.UnimplementedSource
	cfg        config.Source
	subscriber Subscriber
}

// New initialises a new source.
func New() sdk.Source {
	return &Source{}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (s *Source) Configure(_ context.Context, cfgRaw map[string]string) error {
	cfg, err := config.ParseSource(cfgRaw)
	if err != nil {
		return err
	}

	s.cfg = cfg

	return nil
}

// Open initializes a subscriber client.
func (s *Source) Open(ctx context.Context, _ sdk.Position) error {
	subscriber, err := client.NewSubscriber(ctx, s.cfg)
	if err != nil {
		return err
	}

	s.subscriber = subscriber

	return nil
}

// Read returns the next sdk.Record.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	r, err := s.subscriber.Next(ctx)
	if err != nil {
		return sdk.Record{}, err
	}

	return r, nil
}

// Ack indicates successful processing of a message passed.
func (s *Source) Ack(ctx context.Context, _ sdk.Position) error {
	sdk.Logger(ctx).Debug().Msg("got ack")

	return s.subscriber.Ack(ctx)
}

// Teardown releases the GCP subscriber client.
func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("closing the connection to the GCP API service...")

	if s.subscriber != nil {
		return s.subscriber.Stop()
	}

	return nil
}
