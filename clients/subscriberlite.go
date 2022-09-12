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

// SubscriberLite represents queues for messages, an error channel.
type SubscriberLite struct {
	*subscriber
}

// NewSubscriberLite initializes a new subscriber client of GCP Pub/Sub Lite
// and starts receiving a messages to the message queue.
func NewSubscriberLite(ctx context.Context, cfg config.Source) (*SubscriberLite, error) {
	const (
		subscriptionPathFmt  = "projects/%s/locations/%s/subscriptions/%s"
		errMsgNotSupportNack = "pubsublite: subscriber client does not support nack. " +
			"See NackHandler for how to customize nack handling"
	)

	credential, err := cfg.Marshal()
	if err != nil {
		return nil, err
	}

	subLite := &SubscriberLite{
		subscriber: &subscriber{
			msgDeque: deque.New[*pubsub.Message](),
			ackDeque: deque.New[*pubsub.Message](),
			errorCh:  make(chan error),
		},
	}

	client, err := pscompat.NewSubscriberClient(context.Background(),
		fmt.Sprintf(subscriptionPathFmt, cfg.ProjectID, cfg.Location, cfg.SubscriptionID),
		option.WithCredentialsJSON(credential))
	if err != nil {
		return nil, fmt.Errorf("create lite subscriber client: %w", err)
	}

	go func() {
		err = client.Receive(ctx, func(_ context.Context, m *pubsub.Message) {
			subLite.mu.Lock()
			defer subLite.mu.Unlock()

			subLite.msgDeque.PushBack(m)
		})
		// this check is because there is no handling of the Nack method.
		// If processed, the message will be considered acknowledged and will not be sent again
		if err != nil && err.Error() != errMsgNotSupportNack {
			subLite.errorCh <- fmt.Errorf("subscription lite receive: %w", err)

			return
		}

		subLite.errorCh <- nil
	}()

	return subLite, nil
}

// Stop cancels the context to stop the GCP receiver,
// marks all unread messages the client did not receive them,
// and waits the GCP receiver will stop.
func (sl *SubscriberLite) Stop() error {
	return sl.stop()
}
