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
	"encoding/json"
	"io/ioutil"
	"sort"
	"strings"
	"testing"
	"time"

	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	resourcepb "github.com/census-instrumentation/opencensus-proto/gen-go/resource/v1"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/consumerdata"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/translator/conventions"
	"go.opentelemetry.io/collector/translator/internaldata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// Asserts whether dimension sets are equal (i.e. has same sets of dimensions)
func assertDimsEqual(t *testing.T, dims1, dims2 [][]string) {
	// Convert to string for easier sorting
	stringified1 := make([]string, len(dims1))
	stringified2 := make([]string, len(dims2))
	for i, v := range dims1 {
		sort.Strings(v)
		stringified1[i] = strings.Join(v, ",")
	}
	for i, v := range dims2 {
		sort.Strings(v)
		stringified2[i] = strings.Join(v, ",")
	}
	// Sort across dimension sets for equality checking
	sort.Strings(stringified1)
	sort.Strings(stringified2)
	assert.Equal(t, stringified1, stringified2)
}

func TestTranslateOtToCWMetric(t *testing.T) {

	md := consumerdata.MetricsData{
		Node: &commonpb.Node{
			LibraryInfo: &commonpb.LibraryInfo{ExporterVersion: "SomeVersion"},
		},
		Resource: &resourcepb.Resource{
			Labels: map[string]string{
				conventions.AttributeServiceName:      "myServiceName",
				conventions.AttributeServiceNamespace: "myServiceNS",
			},
		},
		Metrics: []*metricspb.Metric{
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_INT64,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanGaugeCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_GAUGE_INT64,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanDoubleCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_DOUBLE,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_DoubleValue{
									DoubleValue: 0.1,
								},
							},
						},
					},
				},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanGaugeDoubleCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_GAUGE_DOUBLE,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_DoubleValue{
									DoubleValue: 0.1,
								},
							},
						},
					},
				},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanTimer",
					Description: "How long the spans take",
					Unit:        "Seconds",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_DistributionValue{
									DistributionValue: &metricspb.DistributionValue{
										Sum:   15.0,
										Count: 5,
										BucketOptions: &metricspb.DistributionValue_BucketOptions{
											Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
												Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
													Bounds: []float64{0, 10},
												},
											},
										},
										Buckets: []*metricspb.DistributionValue_Bucket{
											{
												Count: 0,
											},
											{
												Count: 4,
											},
											{
												Count: 1,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanTimer",
					Description: "How long the spans take",
					Unit:        "Seconds",
					Type:        metricspb.MetricDescriptor_SUMMARY,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan"},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_SummaryValue{
									SummaryValue: &metricspb.SummaryValue{
										Sum: &wrappers.DoubleValue{
											Value: 15.0,
										},
										Count: &wrappers.Int64Value{
											Value: 5,
										},
										Snapshot: &metricspb.SummaryValue_Snapshot{
											PercentileValues: []*metricspb.SummaryValue_Snapshot_ValueAtPercentile{{
												Percentile: 0,
												Value:      1,
											},
												{
													Percentile: 100,
													Value:      5,
												}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	config := &Config{
		Namespace: "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}
	rm := internaldata.OCToMetrics(md).ResourceMetrics().At(0)
	cwm, totalDroppedMetrics := TranslateOtToCWMetric(&rm, config)
	assert.Equal(t, 1, totalDroppedMetrics)
	assert.NotNil(t, cwm)
	assert.Equal(t, 5, len(cwm))
	assert.Equal(t, 1, len(cwm[0].Measurements))

	met := cwm[0]
	assert.Equal(t, met.Fields[OtlibDimensionKey], noInstrumentationLibraryName)
	assert.Equal(t, met.Fields["spanCounter"], 0)

	assert.Equal(t, "myServiceNS/myServiceName", met.Measurements[0].Namespace)
	assert.Equal(t, 4, len(met.Measurements[0].Dimensions))
	dimensions := met.Measurements[0].Dimensions[0]
	sort.Strings(dimensions)
	assert.Equal(t, []string{OtlibDimensionKey, "isItAnError", "spanName"}, dimensions)
	assert.Equal(t, 1, len(met.Measurements[0].Metrics))
	assert.Equal(t, "spanCounter", met.Measurements[0].Metrics[0]["Name"])
	assert.Equal(t, "Count", met.Measurements[0].Metrics[0]["Unit"])
}

func TestTranslateOtToCWMetricWithNameSpace(t *testing.T) {
	md := consumerdata.MetricsData{
		Node: &commonpb.Node{
			LibraryInfo: &commonpb.LibraryInfo{ExporterVersion: "SomeVersion"},
		},
		Resource: &resourcepb.Resource{
			Labels: map[string]string{
				conventions.AttributeServiceName: "myServiceName",
			},
		},
		Metrics: []*metricspb.Metric{},
	}
	config := &Config{
		Namespace: "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}
	rm := internaldata.OCToMetrics(md).ResourceMetrics().At(0)
	cwm, totalDroppedMetrics := TranslateOtToCWMetric(&rm, config)
	assert.Equal(t, 0, totalDroppedMetrics)
	assert.Nil(t, cwm)
	assert.Equal(t, 0, len(cwm))
	md = consumerdata.MetricsData{
		Node: &commonpb.Node{
			LibraryInfo: &commonpb.LibraryInfo{ExporterVersion: "SomeVersion"},
		},
		Resource: &resourcepb.Resource{
			Labels: map[string]string{
				conventions.AttributeServiceNamespace: "myServiceNS",
			},
		},
		Metrics: []*metricspb.Metric{
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_INT64,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_INT64,
				},
				Timeseries: []*metricspb.TimeSeries{},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanGaugeCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_GAUGE_INT64,
				},
				Timeseries: []*metricspb.TimeSeries{},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanGaugeDoubleCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_GAUGE_DOUBLE,
				},
				Timeseries: []*metricspb.TimeSeries{},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanDoubleCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_DOUBLE,
				},
				Timeseries: []*metricspb.TimeSeries{},
			},
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanDoubleCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION,
				},
				Timeseries: []*metricspb.TimeSeries{},
			},
		},
	}
	rm = internaldata.OCToMetrics(md).ResourceMetrics().At(0)
	cwm, totalDroppedMetrics = TranslateOtToCWMetric(&rm, config)
	assert.Equal(t, 0, totalDroppedMetrics)
	assert.NotNil(t, cwm)
	assert.Equal(t, 1, len(cwm))

	met := cwm[0]
	assert.Equal(t, "myServiceNS", met.Measurements[0].Namespace)
}

func TestTranslateOtToCWMetricWithFiltering(t *testing.T) {
	md := consumerdata.MetricsData{
		Node: &commonpb.Node{
			LibraryInfo: &commonpb.LibraryInfo{ExporterVersion: "SomeVersion"},
		},
		Resource: &resourcepb.Resource{
			Labels: map[string]string{
				conventions.AttributeServiceName:      "myServiceName",
				conventions.AttributeServiceNamespace: "myServiceNS",
			},
		},
		Metrics: []*metricspb.Metric{
			{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        "spanCounter",
					Description: "Counting all the spans",
					Unit:        "Count",
					Type:        metricspb.MetricDescriptor_CUMULATIVE_INT64,
					LabelKeys: []*metricspb.LabelKey{
						{Key: "spanName"},
						{Key: "isItAnError"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "testSpan", HasValue: true},
							{Value: "false", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 100,
								},
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
		},
	}

	rm := internaldata.OCToMetrics(md).ResourceMetrics().At(0)

	testCases := []struct {
		testName            string
		metricNameSelectors []string
		dimensions       	[][]string
		numMeasurements		int
	}{
		{
			"With match",
			[]string{"spanCounter"},
			[][]string{
				{"spanName", "isItAnError"},
				{"spanName", "OTLib"},
				{"OTLib", "isItAnError"},
				{"OTLib"},
			},
			1,
		},
		{
			"No match",
			[]string{"invalid"},
			nil,
			0,
		},
	}

	for _, tc := range testCases {
		md := MetricDeclaration{
			Dimensions: [][]string{{"isItAnError", "spanName"}},
			MetricNameSelectors: tc.metricNameSelectors,
		}
		config := &Config{
			Namespace: "",
			DimensionRollupOption: ZeroAndSingleDimensionRollup,
			MetricDeclarations: []*MetricDeclaration{&md},
		}
		md.Init()
		t.Run(tc.testName, func(t *testing.T) {
			cwm, totalDroppedMetrics := TranslateOtToCWMetric(&rm, config)
			assert.Equal(t, 0, totalDroppedMetrics)
			assert.Equal(t, 1, len(cwm))
			assert.NotNil(t, cwm)

			assert.Equal(t, tc.numMeasurements, len(cwm[0].Measurements))

			if tc.numMeasurements > 0 {
				dimensions := cwm[0].Measurements[0].Dimensions
				assertDimsEqual(t, tc.dimensions, dimensions)
			}
		})
	}
}

func TestTranslateCWMetricToEMF(t *testing.T) {
	cwMeasurement := CwMeasurement{
		Namespace:  "test-emf",
		Dimensions: [][]string{{"OTLib"}, {"OTLib", "spanName"}},
		Metrics: []map[string]string{{
			"Name": "spanCounter",
			"Unit": "Count",
		}},
	}
	timestamp := int64(1596151098037)
	fields := make(map[string]interface{})
	fields["OTLib"] = "cloudwatch-otel"
	fields["spanName"] = "test"
	fields["spanCounter"] = 0

	met := &CWMetrics{
		Timestamp:    timestamp,
		Fields:       fields,
		Measurements: []CwMeasurement{cwMeasurement},
	}
	logger := zap.NewNop()
	inputLogEvent := TranslateCWMetricToEMF([]*CWMetrics{met}, logger)

	assert.Equal(t, readFromFile("testdata/testTranslateCWMetricToEMF.json"), *inputLogEvent[0].InputLogEvent.Message, "Expect to be equal")
}

func TestTranslateCWMetricToEMFNoMeasurements(t *testing.T) {
	timestamp := int64(1596151098037)
	fields := make(map[string]interface{})
	fields["OTLib"] = "cloudwatch-otel"
	fields["spanName"] = "test"
	fields["spanCounter"] = 0

	met := &CWMetrics{
		Timestamp:    timestamp,
		Fields:       fields,
		Measurements: nil,
	}
	obs, logs := observer.New(zap.WarnLevel)
	logger := zap.New(obs)
	inputLogEvent := TranslateCWMetricToEMF([]*CWMetrics{met}, logger)
	expected := "{\"OTLib\":\"cloudwatch-otel\",\"spanCounter\":0,\"spanName\":\"test\"}"

	assert.Equal(t, expected, *inputLogEvent[0].InputLogEvent.Message)

	// Check logged warning message
	fieldsStr, _ := json.Marshal(fields)
	expectedLogs := []observer.LoggedEntry{{
		Entry:   zapcore.Entry{Level: zap.WarnLevel, Message: "Dropped metric due to no matching metric declarations"},
		Context: []zapcore.Field{zap.String("labels", string(fieldsStr))},
	}}
	assert.Equal(t, 1, logs.Len())
	assert.Equal(t, expectedLogs, logs.AllUntimed())
}

func TestGetCWMetrics(t *testing.T) {

}

func TestBuildCWMetric(t *testing.T) {
	namespace := "Namespace"
	OTLib := "OTLib"
	metricName := "metric1"
	metricValue := int64(-17)
	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetName(metricName)
	metricSlice := []map[string]string{{"Name": metricName}}
	testCases := []struct {
		testName 				string
		labels 					map[string]string
		dimensionRollupOption 	string
		expectedDims 			[][]string
	}{
		{
			"Single label w/ no rollup",
			map[string]string{"a": "foo"},
			"",
			[][]string{
				{"a", OTLib},
			},
		},
		{
			"Single label w/ single rollup",
			map[string]string{"a": "foo"},
			SingleDimensionRollupOnly,
			[][]string{
				{"a", OTLib},
				{OTLib, "a"},
			},
		},
		{
			"Single label w/ zero + single rollup",
			map[string]string{"a": "foo"},
			ZeroAndSingleDimensionRollup,
			[][]string{
				{"a", OTLib},
				{OTLib, "a"},
				{OTLib},
			},
		},
		{
			"Multiple label w/ no rollup",
			map[string]string{
				"a": "foo",
				"b": "bar",
				"c": "car",
			},
			"",
			[][]string{
				{"a", "b", "c", OTLib},
			},
		},
		{
			"Multiple label w/ rollup",
			map[string]string{
				"a": "foo",
				"b": "bar",
				"c": "car",
			},
			ZeroAndSingleDimensionRollup,
			[][]string{
				{"a", "b", "c", OTLib},
				{OTLib, "a"},
				{OTLib, "b"},
				{OTLib, "c"},
				{OTLib},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			dp := pdata.NewIntDataPoint()
			dp.InitEmpty()
			dp.LabelsMap().InitFromMap(tc.labels)
			dp.SetValue(metricValue)
			config := &Config{
				Namespace: namespace,
				DimensionRollupOption: tc.dimensionRollupOption,
			}

			expectedFields := map[string]interface{}{
				OtlibDimensionKey: OTLib,
				metricName: 0,
			}
			for k, v := range tc.labels {
				expectedFields[k] = v
			}

			cwMetric := buildCWMetric(dp, &metric, namespace, metricSlice, OTLib, config)
			
			// Check fields
			assert.Equal(t, expectedFields, cwMetric.Fields)

			// Check CW measurement
			assert.Equal(t, 1, len(cwMetric.Measurements))
			cwMeasurement := cwMetric.Measurements[0]
			assert.Equal(t, namespace, cwMeasurement.Namespace)
			assert.Equal(t, metricSlice, cwMeasurement.Metrics)
			assertDimsEqual(t, tc.expectedDims, cwMeasurement.Dimensions)
		})
	}
}

func TestBuildCWMetricWithMetricDeclarations(t *testing.T) {
	namespace := "Namespace"
	OTLib := "OTLib"
	metricName := "metric1"
	metricValue := int64(-17)
	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetName(metricName)
	metricSlice := []map[string]string{{"Name": metricName}}
	testCases := []struct {
		testName 				string
		labels 					map[string]string
		metricDeclarations 		[]*MetricDeclaration
		dimensionRollupOption 	string
		expectedDims 			[][][]string
	}{
		{
			"Single label w/ no rollup",
			map[string]string{"a": "foo"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][][]string{{{"a"}}},
		},
		{
			"Single label + OTLib w/ no rollup",
			map[string]string{"a": "foo"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a", OTLib}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][][]string{{{"a", OTLib}}},
		},
		{
			"Single label w/ single rollup",
			map[string]string{"a": "foo"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			SingleDimensionRollupOnly,
			[][][]string{{{"a"}, {"a", OTLib}}},
		},
		{
			"Single label w/ zero/single rollup",
			map[string]string{"a": "foo"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			[][][]string{{{"a"}, {"a", OTLib}, {OTLib}}},
		},
		{
			"No matching metric name",
			map[string]string{"a": "foo"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a"}},
					MetricNameSelectors: []string{"invalid"},
				},
			},
			"",
			nil,
		},
		{
			"multiple labels w/ no rollup",
			map[string]string{"a": "foo", "b": "bar"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][][]string{{{"a"}}},
		},
		{
			"multiple labels w/ rollup",
			map[string]string{"a": "foo", "b": "bar"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			[][][]string{{
				{"a"},
				{OTLib, "a"},
				{OTLib, "b"},
				{OTLib},
			}},
		},
		{
			"multiple labels + multiple dimensions w/ no rollup",
			map[string]string{"a": "foo", "b": "bar"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a", "b"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][][]string{{{"a", "b"}, {"b"}}},
		},
		{
			"multiple labels + multiple dimensions + OTLib w/ no rollup",
			map[string]string{"a": "foo", "b": "bar"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a", "b"}, {"b", OTLib}, {OTLib}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][][]string{{{"a", "b"}, {"b", OTLib}, {OTLib}}},
		},
		{
			"multiple labels + multiple dimensions w/ rollup",
			map[string]string{"a": "foo", "b": "bar"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a", "b"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			[][][]string{{
				{"a", "b"},
				{"b"},
				{OTLib, "a"},
				{OTLib, "b"},
				{OTLib},
			}},
		},
		{
			"multiple labels, multiple dimensions w/ invalid dimension",
			map[string]string{"a": "foo", "b": "bar"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a", "b", "c"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			[][][]string{{
				{"b"},
				{OTLib, "a"},
				{OTLib, "b"},
				{OTLib},
			}},
		},
		{
			"multiple labels, multiple dimensions w/ missing dimension",
			map[string]string{"a": "foo", "b": "bar", "c": "car"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a", "b"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			[][][]string{{
				{"a", "b"},
				{"b"},
				{OTLib, "a"},
				{OTLib, "b"},
				{OTLib, "c"},
				{OTLib},
			}},
		},
		{
			"multiple metric declarations w/ no rollup",
			map[string]string{"a": "foo", "b": "bar", "c": "car"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a", "b"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
				{
					Dimensions: [][]string{{"a", "c"}, {"b"}, {"c"}},
					MetricNameSelectors: []string{metricName},
				},
				{
					Dimensions: [][]string{{"a", "d"}, {"b", "c"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][][]string{
				{{"a", "b"}, {"b"}},
				{{"a", "c"}, {"b"}, {"c"}},
				{{"b", "c"}},
			},
		},
		{
			"multiple metric declarations w/ rollup",
			map[string]string{"a": "foo", "b": "bar", "c": "car"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a", "b"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
				{
					Dimensions: [][]string{{"a", "c"}, {"b"}, {"c"}},
					MetricNameSelectors: []string{metricName},
				},
				{
					Dimensions: [][]string{{"a", "d"}, {"b", "c"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			[][][]string{
				{
					{"a", "b"},
					{"b"},
					{OTLib, "a"},
					{OTLib, "b"},
					{OTLib, "c"},
					{OTLib},
				},
				{
					{"a", "c"},
					{"b"},
					{"c"},
					{OTLib, "a"},
					{OTLib, "b"},
					{OTLib, "c"},
					{OTLib},
				},
				{
					{"b", "c"},
					{OTLib, "a"},
					{OTLib, "b"},
					{OTLib, "c"},
					{OTLib},
				},
			},
		},
		{
			"remove measurements with no dimensions",
			map[string]string{"a": "foo", "b": "bar", "c": "car"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a", "b"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
				{
					Dimensions: [][]string{{"a", "d"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			[][][]string{
				{{"a", "b"}, {"b"}},
			},
		},
		{
			"multiple declarations w/ no dimensions",
			map[string]string{"a": "foo", "b": "bar", "c": "car"},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a", "e"}, {"d"}},
					MetricNameSelectors: []string{metricName},
				},
				{
					Dimensions: [][]string{{"a", "d"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			"",
			nil,
		},
		{
			"no labels",
			map[string]string{},
			[]*MetricDeclaration{
				{
					Dimensions: [][]string{{"a", "b", "c"}, {"b"}},
					MetricNameSelectors: []string{metricName},
				},
			},
			ZeroAndSingleDimensionRollup,
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			dp := pdata.NewIntDataPoint()
			dp.InitEmpty()
			dp.LabelsMap().InitFromMap(tc.labels)
			dp.SetValue(metricValue)
			config := &Config{
				Namespace: namespace,
				DimensionRollupOption: tc.dimensionRollupOption,
				MetricDeclarations: tc.metricDeclarations,
			}
			for _, decl := range tc.metricDeclarations {
				decl.Init()
			}

			expectedFields := map[string]interface{}{
				OtlibDimensionKey: OTLib,
				metricName: 0,
			}
			for k, v := range tc.labels {
				expectedFields[k] = v
			}

			cwMetric := buildCWMetric(dp, &metric, namespace, metricSlice, OTLib, config)
			
			// Check fields
			assert.Equal(t, expectedFields, cwMetric.Fields)

			// Check CW measurement
			assert.Equal(t, len(tc.expectedDims), len(cwMetric.Measurements))
			for i, dimensions := range tc.expectedDims {
				cwMeasurement := cwMetric.Measurements[i]
				assert.Equal(t, namespace, cwMeasurement.Namespace)
				assert.Equal(t, metricSlice, cwMeasurement.Metrics)
				assertDimsEqual(t, dimensions, cwMeasurement.Dimensions)
			}
		})
	}
}

func TestCalculateRate(t *testing.T) {
	prevValue := int64(0)
	curValue := int64(10)
	fields := make(map[string]interface{})
	fields["OTLib"] = "cloudwatch-otel"
	fields["spanName"] = "test"
	fields["spanCounter"] = prevValue
	fields["type"] = "Int64"
	prevTime := time.Now().UnixNano() / int64(time.Millisecond)
	curTime := time.Unix(0, prevTime*int64(time.Millisecond)).Add(time.Second*10).UnixNano() / int64(time.Millisecond)
	rate := calculateRate(fields, prevValue, prevTime)
	assert.Equal(t, 0, rate)
	rate = calculateRate(fields, curValue, curTime)
	assert.Equal(t, int64(1), rate)

	prevDoubleValue := 0.0
	curDoubleValue := 5.0
	fields["type"] = "Float64"
	rate = calculateRate(fields, prevDoubleValue, prevTime)
	assert.Equal(t, 0, rate)
	rate = calculateRate(fields, curDoubleValue, curTime)
	assert.Equal(t, 0.5, rate)
}

func readFromFile(filename string) string {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	str := string(data)
	return str
}
