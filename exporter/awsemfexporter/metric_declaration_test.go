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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestInit(t *testing.T) {
	md := &MetricDeclaration{
		MetricNameSelectors: []string{"a", "b", "aa"},
	}
	logger := zap.NewNop()
	err := md.Init(logger)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(md.metricRegexList))

	md = &MetricDeclaration{
		Dimensions: [][]string{
			{"foo"},
			{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
		},
		MetricNameSelectors: []string{"a.*", "b$", "aa+"},
	}
	err = md.Init(logger)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(md.metricRegexList))
	assert.Equal(t, 2, len(md.Dimensions))

	// Test removal of dimension sets with more than 10 elements
	md = &MetricDeclaration{
		Dimensions: [][]string{
			{"foo"},
			{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
		},
		MetricNameSelectors: []string{"a.*", "b$", "aa+"},
	}
	obs, logs := observer.New(zap.WarnLevel)
	logger = zap.New(obs)
	err = md.Init(logger)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(md.metricRegexList))
	assert.Equal(t, 1, len(md.Dimensions))
	// Check logged warning message
	expectedLogs := []observer.LoggedEntry{{
		Entry:   zapcore.Entry{Level: zap.WarnLevel, Message: "Dropped dimension set: > 10 dimensions specified."},
		Context: []zapcore.Field{zap.String("dimensions", "a,b,c,d,e,f,g,h,i,j,k")},
	}}
	assert.Equal(t, 1, logs.Len())
	assert.Equal(t, expectedLogs, logs.AllUntimed())

	// Test invalid metric declaration
	md = &MetricDeclaration{}
	err = md.Init(logger)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Invalid metric declaration: no metric name selectors defined.")
}

func TestMatches(t *testing.T) {
	md := &MetricDeclaration{
		MetricNameSelectors: []string{"^a+$", "^b.*$", "^ac+a$"},
	}
	logger := zap.NewNop()
	err := md.Init(logger)
	assert.Nil(t, err)

	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetName("a")
	assert.True(t, md.Matches(&metric))

	metric.SetName("aa")
	assert.True(t, md.Matches(&metric))

	metric.SetName("aaaa")
	assert.True(t, md.Matches(&metric))

	metric.SetName("aaab")
	assert.False(t, md.Matches(&metric))

	metric.SetName("b")
	assert.True(t, md.Matches(&metric))

	metric.SetName("ba")
	assert.True(t, md.Matches(&metric))

	metric.SetName("c")
	assert.False(t, md.Matches(&metric))

	metric.SetName("aca")
	assert.True(t, md.Matches(&metric))

	metric.SetName("accca")
	assert.True(t, md.Matches(&metric))
}

func TestExtractDimensions(t *testing.T) {
	testCases := []struct {
		testName            string
		dimensions          [][]string
		labels              map[string]interface{}
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
		{
			"no dimensions",
			[][]string{},
			map[string]interface{}{
				"a": "foo",
			},
			nil,
		},
		{
			"empty dimension set",
			[][]string{{}},
			map[string]interface{}{
				"a": "foo",
			},
			nil,
		},
	}
	logger := zap.NewNop()

	for _, tc := range testCases {
		md := MetricDeclaration{
			Dimensions:          tc.dimensions,
			MetricNameSelectors: []string{"foo"},
		}
		err := md.Init(logger)
		assert.Nil(t, err)
		t.Run(tc.testName, func(t *testing.T) {
			dimensions := md.ExtractDimensions(tc.labels)
			assertDimsEqual(t, tc.extractedDimensions, dimensions)
		})
	}
}

func TestProcessMetricDeclarations(t *testing.T) {
	mds := []*MetricDeclaration{
		{
			Dimensions:          [][]string{{"dim1", "dim2"}},
			MetricNameSelectors: []string{"a", "b"},
		},
		{
			Dimensions:          [][]string{{"dim1"}},
			MetricNameSelectors: []string{"aa", "b"},
		},
		{
			Dimensions:          [][]string{{"dim1", "dim2"}, {"dim1"}},
			MetricNameSelectors: []string{"a"},
		},
	}
	logger := zap.NewNop()
	for _, decl := range mds {
		err := decl.Init(logger)
		assert.Nil(t, err)
	}
	testCases := []struct {
		testName       string
		metricName     string
		labels         map[string]interface{}
		dimensionsList [][][]string
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
