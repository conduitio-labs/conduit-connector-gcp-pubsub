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

import "fmt"

// RequiredErr returns the formatted required field error.
func RequiredErr(name string) error {
	return fmt.Errorf(requiredErrMsg, name)
}

// InvalidEmailErr returns the formatted email field error.
func InvalidEmailErr(name string) error {
	return fmt.Errorf(invalidEmailErrMsg, name)
}

// InvalidNameErr returns the formatted invalid name error.
func InvalidNameErr(name string) error {
	return fmt.Errorf(invalidNameErrMsg, name)
}

// InvalidIntegerTypeErr returns the formatted invalid integer type error.
func InvalidIntegerTypeErr(name string) error {
	return fmt.Errorf(invalidIntegerTypeErrMsg, name)
}

// InvalidTimeDurationTypeErr returns the formatted invalid time duration type error.
func InvalidTimeDurationTypeErr(name string) error {
	return fmt.Errorf(invalidTimeDurationErrMsg, name)
}

// OutOfRangeErr returns the formatted out of range error.
func OutOfRangeErr(name string) error {
	return fmt.Errorf(outOfRangeErrMsg, name)
}
