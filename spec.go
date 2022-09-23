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

package gcppubsub

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// version is set during the build process (i.e. the Makefile).
// It follows Go's convention for module version, where the version
// starts with the letter v, followed by a semantic version.
var version = "v0.0.0-dev"

// Specification returns specification of the connector.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "gcp-pub-sub",
		Summary: "A GCP Pub/Sub source and destination plugin for Conduit, written in Go.",
		Description: "The Google Cloud Platform Pub/Sub connector is one of Conduit plugins. " +
			"It provides a source and a destination GCP Pub/Sub and Pub/Sub Lite connector.",
		Version: version,
		Author:  "Meroxa, Inc.",
	}
}
