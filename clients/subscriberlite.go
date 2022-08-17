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

package clients

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/pscompat"
	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/config"
	"github.com/gammazero/deque"
	"google.golang.org/api/option"
)

const (
	subscriptionPathFmt  = "projects/%s/locations/%s/subscriptions/%s"
	errMsgNotSupportNack = "pubsublite: subscriber client does not support nack. " +
		"See NackHandler for how to customize nack handling"
)

// SubscriberLite represents a struct with a GCP Pub/Sub Lite client,
// queues for messages, channels and a function to cancel the context.
type SubscriberLite struct {
	client *pscompat.SubscriberClient
	*subscriber
}

// NewSubscriberLite initializes a new subscriber client of GCP Pub/Sub Lite
// and starts receiving a messages to the queue.
func NewSubscriberLite(ctx context.Context, cfg config.Source) (*SubscriberLite, error) {
	credential, err := cfg.Marshal()
	if err != nil {
		return nil, err
	}

	subscriptionPath := fmt.Sprintf(subscriptionPathFmt, cfg.ProjectID, cfg.Location, cfg.SubscriptionID)

	client, err := pscompat.NewSubscriberClient(ctx, subscriptionPath, option.WithCredentialsJSON(credential))
	if err != nil {
		return nil, fmt.Errorf("create lite subscriber client: %w", err)
	}

	cctx, cancel := context.WithCancel(ctx)

	sl := &SubscriberLite{
		client: client,
		subscriber: &subscriber{
			msgDeque:  deque.New[*pubsub.Message](),
			ackDeque:  deque.New[*pubsub.Message](),
			errorCh:   make(chan error),
			ctxCancel: cancel,
		},
	}

	go func() {
		err = sl.client.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
			sl.lock.Lock()
			defer sl.lock.Unlock()

			sl.msgDeque.PushBack(m)
		})
		// this check is because there is no handling of the Nack method.
		// If processed, the message will be considered acknowledged and will not be sent again
		if err != nil && err.Error() != errMsgNotSupportNack {
			sl.errorCh <- fmt.Errorf("subscription receive: %w", err)
		}

		sl.errorCh <- nil
	}()

	return sl, nil
}

// Stop cancels the context to stop the GCP receiver,
// marks all unread messages the client did not receive them,
// and waits the GCP receiver will stop.
func (sl *SubscriberLite) Stop() error {
	return sl.stop()
}
