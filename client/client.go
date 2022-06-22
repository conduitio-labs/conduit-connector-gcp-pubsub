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

package client

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/conduitio/conduit-connector-gcp-pubsub/config"
	"google.golang.org/api/option"
)

// pubSub represents a struct with a GCP Pub/Sub client.
type pubSub struct {
	client *pubsub.Client
}

// newClient initializes a new GCP Pub/Sub client.
func newClient(ctx context.Context, cfg config.General) (*pubSub, error) {
	credential, err := cfg.Marshal()
	if err != nil {
		return nil, err
	}

	cli, err := pubsub.NewClient(ctx, cfg.ProjectID, option.WithCredentialsJSON(credential))
	if err != nil {
		return nil, fmt.Errorf("create a new pubsub client: %w", err)
	}

	return &pubSub{
		client: cli,
	}, nil
}

// close releases any resources held by the client,
// such as memory and goroutines.
func (ps pubSub) close() error {
	if ps.client == nil {
		return nil
	}

	return ps.client.Close()
}
