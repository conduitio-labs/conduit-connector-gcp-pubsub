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

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/conduitio-labs/conduit-connector-gcp-pubsub/models"
	v "github.com/go-playground/validator/v10"
	"go.uber.org/multierr"
)

var (
	validatorInstance *v.Validate
	once              sync.Once

	isObjectNameValid = regexp.MustCompile(`^[a-zA-Z][a-zA-Z\d-._~%+]{2,254}$`).MatchString
)

// Get initializes and registers validation tags once, and returns validator instance.
func Get() *v.Validate {
	once.Do(func() {
		validatorInstance = v.New()

		err := validatorInstance.RegisterValidation("object_name", validateObjectName)
		if err != nil {
			return
		}
	})

	return validatorInstance
}

func Validate(s interface{}) error {
	var err error

	validationErr := Get().Struct(s)
	if validationErr != nil {
		if _, ok := validationErr.(*v.InvalidValidationError); ok {
			return fmt.Errorf("validate general config struct: %w", validationErr)
		}

		for _, e := range validationErr.(v.ValidationErrors) {
			switch e.ActualTag() {
			case "required":
				err = multierr.Append(err, RequiredErr(models.ConfigKeyName(e.Field())))
			case "email":
				err = multierr.Append(err, InvalidEmailErr(models.ConfigKeyName(e.Field())))
			case "object_name":
				err = multierr.Append(err, InvalidNameErr(models.ConfigKeyName(e.Field())))
			case "gte", "lte":
				err = multierr.Append(err, OutOfRangeErr(models.ConfigKeyName(e.Field())))
			}
		}
	}

	return err
}

func validateObjectName(fl v.FieldLevel) bool {
	if strings.HasPrefix(strings.ToLower(fl.Field().String()), googPrefix) {
		return false
	}

	return isObjectNameValid(fl.Field().String())
}
