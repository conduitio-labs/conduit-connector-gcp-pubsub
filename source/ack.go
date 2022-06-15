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

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// ack indicates successful processing of a Message passed.
func (s *Source) ack(ctx context.Context) error {
	if s.pubSub == nil {
		return errPubsubIsNil
	}

	select {
	case msg := <-s.pubSub.AckMessagesCh:
		sdk.Logger(ctx).Debug().Str("message_id", msg.ID).Msg("got ack")

		msg.Ack()

		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
