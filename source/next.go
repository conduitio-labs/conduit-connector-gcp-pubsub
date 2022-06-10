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

const (
	idKey       = "id"
	actionKey   = "action"
	insertValue = "insert"
)

// next receives and returns a record.
func (s *Source) next(ctx context.Context) (sdk.Record, error) {
	select {
	case msg := <-s.pubSub.MessagesCh:
		s.pubSub.AckMessagesCh <- msg

		return sdk.Record{
			Position: sdk.Position(msg.ID),
			Metadata: map[string]string{
				actionKey: insertValue,
			},
			CreatedAt: msg.PublishTime,
			Key: sdk.StructuredData{
				idKey: msg.ID,
			},
			Payload: sdk.RawData(msg.Data),
		}, nil
	case err := <-s.pubSub.ErrorCh:
		return sdk.Record{}, err
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		return sdk.Record{}, sdk.ErrBackoffRetry
	}
}
