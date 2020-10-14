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

func TestExtractDimensions(t *testing.T) {
	testCases := []struct{
		testName 			string
		dimensions 			[][]string
		labels 				map[string]interface{}
		extractedDimensions [][]string
	}{
		{
			"matches single dimension set exactly",
			[][]string{{"a", "b"}},
			map[string]interface{}{
				"a": "foo",
				"b": "bar",
			},
			[][]string{{"a", "b"}},
		},
		{
			"matches subset of single dimension set",
			[][]string{{"a"}},
			map[string]interface{}{
				"a": "foo",
				"b": "bar",
			},
			[][]string{{"a"}},
		},
		{
			"does not match single dimension set",
			[][]string{{"a", "b"}},
			map[string]interface{}{
				"b": "bar",
			},
			nil,
		},
		{
			"matches multiple dimension sets",
			[][]string{{"a", "b"}, {"a"}},
			map[string]interface{}{
				"a": "foo",
				"b": "bar",
			},
			[][]string{{"a", "b"}, {"a"}},
		},
		{
			"matches one of multiple dimension sets",
			[][]string{{"a", "b"}, {"a"}},
			map[string]interface{}{
				"a": "foo",
			},
			[][]string{{"a"}},
		},
	}

	for _, tc := range testCases {
		md := MetricDeclaration{
			Dimensions: tc.dimensions,
		}
		t.Run(tc.testName, func(t *testing.T) {
			dimensions := md.ExtractDimensions(tc.labels)
			assertDimsEqual(t, tc.extractedDimensions, dimensions)
		})
	}
}

func TestProcessMetricDeclarations(t *testing.T) {
	mds := []*MetricDeclaration{
		{
			Dimensions: [][]string{{"dim1", "dim2"}},
			MetricNameSelectors: []string{"a", "b"},
		},
		{
			Dimensions: [][]string{{"dim1"}},
			MetricNameSelectors: []string{"aa", "b"},
		},
		{
			Dimensions: [][]string{{"dim1", "dim2"}, {"dim1"}},
			MetricNameSelectors: []string{"a"},
		},
	}
	testCases := []struct{
		testName 		string
		metricName		string
		labels			map[string]interface{}
		dimensionsList 	[][][]string
	}{
		{
			"Matching multiple dimensions 1",
			"a",
			map[string]interface{}{
				"dim1": "foo",
				"dim2": "bar",
			},
			[][][]string{
				{{"dim1", "dim2"}},
				{{"dim1", "dim2"}, {"dim1"}},
			},
		},
		{
			"Matching multiple dimensions 2",
			"b",
			map[string]interface{}{
				"dim1": "foo",
				"dim2": "bar",
			},
			[][][]string{
				{{"dim1", "dim2"}},
				{{"dim1"}},
			},
		},
		{
			"Match single dimension set",
			"a",
			map[string]interface{}{
				"dim1": "foo",
			},
			[][][]string{
				{{"dim1"}},
			},
		},
		{
			"No matching dimension set",
			"a",
			map[string]interface{}{
				"dim2": "bar",
			},
			nil,
		},
		{
			"No matching metric name",
			"c",
			map[string]interface{}{
				"dim1": "foo",
			},
			nil,
		},
	}

	for _, tc := range testCases {
		metric := pdata.NewMetric()
		metric.InitEmpty()
		metric.SetName(tc.metricName)
		t.Run(tc.testName, func(t *testing.T) {
			dimensionsList := processMetricDeclarations(mds, &metric, tc.labels)
			assert.Equal(t, len(tc.dimensionsList), len(dimensionsList))
			for i, dimensions := range dimensionsList {
				assertDimsEqual(t, tc.dimensionsList[i], dimensions)
			}
		})
	}
}
