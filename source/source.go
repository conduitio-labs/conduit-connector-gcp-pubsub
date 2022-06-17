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
	"time"

	"github.com/conduitio/conduit-connector-gcp-pubsub/clients"
	"github.com/conduitio/conduit-connector-gcp-pubsub/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// A Source represents the source connector.
type Source struct {
	sdk.UnimplementedSource
	cfg    config.Source
	pubSub *clients.PubSub
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

// Open initializes a Pub/Sub client.
func (s *Source) Open(ctx context.Context, _ sdk.Position) error {
	pubSub, err := clients.NewClient(ctx, s.cfg)
	if err != nil {
		return err
	}

	s.pubSub = pubSub

	return nil
}

// Read returns the next sdk.Record.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	r, err := s.next(ctx)
	if err != nil {
		return sdk.Record{}, err
	}

	return r, nil
}

// Ack indicates successful processing of a message passed.
func (s *Source) Ack(ctx context.Context, _ sdk.Position) error {
	return s.ack(ctx)
}

// Teardown releases any resources held by the GCP Pub/Sub client.
func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("closing the connection to the GCP API service...")

	const waitTime = 100 * time.Millisecond

	if s.pubSub != nil {
		time.Sleep(waitTime)

		return s.pubSub.Cli.Close()
	}

	return nil
}
