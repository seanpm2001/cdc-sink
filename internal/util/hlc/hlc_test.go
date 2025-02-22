// Copyright 2023 The Cockroach Authors
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
//
// SPDX-License-Identifier: Apache-2.0

package hlc

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	a := assert.New(t)

	a.True(Compare(Time{1, 1}, Time{1, 1}) == 0)

	a.True(Compare(Time{2, 1}, Time{1, 1}) > 0)
	a.True(Compare(Time{1, 1}, Time{2, 1}) < 0)

	a.True(Compare(Time{1, 2}, Time{1, 1}) > 0)
	a.True(Compare(Time{1, 1}, Time{1, 2}) < 0)
}

func TestParse(t *testing.T) {
	// Implementation copied from sink_table_test.go

	tests := []struct {
		testcase        string
		expectedPass    bool
		expectedNanos   int64
		expectedLogical int
	}{
		{"", false, 0, 0},
		{".", false, 0, 0},
		{"1233", false, 0, 0},
		{".1233", false, 0, 0},
		{"123.123", false, 123, 123},
		{"0.0000000000", true, 0, 0},
		{"1586019746136571000.0000000000", true, 1586019746136571000, 0},
		{"1586019746136571000.0000000001", true, 1586019746136571000, 1},
		{"9223372036854775807.2147483647", true, math.MaxInt64, math.MaxInt32},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d - %s", i, test.testcase), func(t *testing.T) {
			a := assert.New(t)
			actual, actualErr := Parse(test.testcase)
			if test.expectedPass && a.NoError(actualErr) {
				a.Equal(test.expectedNanos, actual.Nanos(), "nanos")
				a.Equal(test.expectedLogical, actual.Logical(), "logical")
				a.Equal(test.testcase, actual.String())
				bytes, err := actual.MarshalJSON()
				a.NoError(err)
				a.Equal([]byte(fmt.Sprintf("%q", test.testcase)), bytes)

				var unmarshaled Time
				a.NoError(json.Unmarshal(bytes, &unmarshaled))
				a.Equal(actual, unmarshaled)
			} else if !test.expectedPass {
				a.Error(actualErr)
			}
		})
	}
}
