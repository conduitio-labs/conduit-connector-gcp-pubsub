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

package validator

const (
	googPrefix = "goog"

	requiredErrMsg     = "%q value must be set"
	invalidEmailErrMsg = "%q value must be a valid email address"
	invalidNameErrMsg  = "%q must be 3-255 characters, start with a letter, and contain only the following characters: " +
		"letters, numbers, dashes (-), periods (.), underscores (_), tildes (~), percents (%%) or plus signs (+). " +
		"Cannot start with goog"
	invalidIntegerTypeErrMsg  = "%q value must be an integer"
	invalidTimeDurationErrMsg = "%q value must be a time duration"
	outOfRangeErrMsg          = "%q is out of range"
)
