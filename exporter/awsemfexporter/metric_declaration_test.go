// Copyright 2020, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package awsemfexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/pdata"
)

func TestMatches(t *testing.T) {
	md := &MetricDeclaration{
		MetricNameSelectors: []string{"a", "b", "aa"},
	}

	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetName("a")
	assert.True(t, md.Matches(&metric))

	metric.SetName("b")
	assert.True(t, md.Matches(&metric))

	metric.SetName("c")
	assert.False(t, md.Matches(&metric))
	
	metric.SetName("aa")
	assert.True(t, md.Matches(&metric))

	metric.SetName("aaa")
	assert.False(t, md.Matches(&metric))
}

func TestFilterMetricDeclarations(t *testing.T) {
	md1 := MetricDeclaration{
		MetricNameSelectors: []string{"a", "b", "aa"},
	}
	md2 := MetricDeclaration{
		MetricNameSelectors: []string{"c", "d", "aa"},
	}
	md3 := MetricDeclaration{
		MetricNameSelectors: []string{"c", "d", "e"},
	}
	mds := []MetricDeclaration{md1, md2, md3}

	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetName("aa")

	filtered := filterMetricDeclarations(mds, &metric)

	assert.Contains(t, filtered, md1)
	assert.Contains(t, filtered, md2)
	assert.NotContains(t, filtered, md3)
}
