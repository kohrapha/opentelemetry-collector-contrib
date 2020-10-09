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
	"go.opentelemetry.io/collector/translator/conventions"
	"go.opentelemetry.io/collector/translator/internaldata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// Sorts 2D slice of dims for equality checking
func dimsSorter(dims [][]string) func(a, b int) bool {
	stringified := make([]string, len(dims))
	for i, v := range dims {
		stringified[i] = strings.Join(v, ",")
	}
	return func(i, j int) bool {
		return stringified[i] > stringified[j]
	}
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
		MetricDeclarations: []MetricDeclaration{
			{
				MetricNameSelectors: []string{
					"spanCounter",
					"spanGaugeCounter",
					"spanDoubleCounter",
					"spanGaugeDoubleCounter",
					"spanTimer",
				},
			},
		},
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
		MetricDeclarations: []MetricDeclaration{},
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
	config.MetricDeclarations = []MetricDeclaration{
		{
			MetricNameSelectors: []string{
				"spanCounter",
				"spanGaugeCounter",
				"spanDoubleCounter",
				"spanGaugeDoubleCounter",
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
		},
	}

	rm := internaldata.OCToMetrics(md).ResourceMetrics().At(0)

	testCases := []struct {
		testName            string
		metricNameSelectors []string
		numMetrics          int
	}{
		{
			"No filtering",
			[]string{"spanCounter", "spanGaugeCounter", "spanDoubleCounter", "spanGaugeDoubleCounter"},
			4,
		},
		{
			"Only double counters",
			[]string{"spanDoubleCounter", "spanGaugeDoubleCounter"},
			2,
		},
		{
			"Remove all",
			[]string{},
			0,
		},
	}

	for _, tc := range testCases {
		config := &Config{
			Namespace: "",
			DimensionRollupOption: ZeroAndSingleDimensionRollup,
			MetricDeclarations: []MetricDeclaration{
				{MetricNameSelectors: tc.metricNameSelectors},
			},
		}
		t.Run(tc.testName, func(t *testing.T) {
			cwm, totalDroppedMetrics := TranslateOtToCWMetric(&rm, config)
			assert.Equal(t, 0, totalDroppedMetrics)
			assert.Equal(t, tc.numMetrics, len(cwm))
			if tc.numMetrics > 0 {
				assert.NotNil(t, cwm)
				assert.Equal(t, 1, len(cwm[0].Measurements))
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

func TestTranslateCWMetricToEMFNoDimensions(t *testing.T) {
	cwMeasurement := CwMeasurement{
		Namespace:  "test-emf",
		Dimensions: [][]string{},
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
	obs, logs := observer.New(zap.WarnLevel)
	logger := zap.New(obs)
	inputLogEvent := TranslateCWMetricToEMF([]*CWMetrics{met}, logger)
	expected := "{\"OTLib\":\"cloudwatch-otel\",\"spanCounter\":0,\"spanName\":\"test\"}"

	assert.Equal(t, expected, *inputLogEvent[0].InputLogEvent.Message)

	// Check logged warning message
	expectedLogs := []observer.LoggedEntry{{
		Entry:   zapcore.Entry{Level: zap.WarnLevel, Message: "Dropped metric due to no matching metric declaration"},
		Context: []zapcore.Field{zap.String("metricName", "spanCounter")},
	}}
	assert.Equal(t, 1, logs.Len())
	assert.Equal(t, expectedLogs, logs.AllUntimed())
}

func TestGetCWMetrics(t *testing.T) {

}

func TestCreateDimensions(t *testing.T) {
	OTLib := "OTLib"
	mds := []MetricDeclaration{
		{
			MetricNameSelectors: []string{"a", "b"},
		},
	}
	testCases := []struct {
		testName 	 string
		labels   	 map[string]interface{}
		expectedDims [][]string
	}{
		{
			"single label",
			map[string]interface{}{"a": "foo"},
			[][]string{
				{"a", OTLib},
				{OTLib},
				{OTLib, "a"},
			},
		},
		{
			"multiple labels",
			map[string]interface{}{"a": "foo", "b": "bar"},
			[][]string{
				{"a", "b", OTLib},
				{OTLib},
				{OTLib, "a"},
				{OTLib, "b"},
			},
		},
		{
			"no labels",
			map[string]interface{}{},
			[][]string{
				{OTLib},
			},
		},
	}

	for _, tc := range testCases {
		dimensions := createDimensions(mds, tc.labels, ZeroAndSingleDimensionRollup)

		// Sort slice for equality check
		sort.Slice(tc.expectedDims, dimsSorter(tc.expectedDims))
		sort.Slice(dimensions, dimsSorter(dimensions))

		assert.Equal(t, tc.expectedDims, dimensions)
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
