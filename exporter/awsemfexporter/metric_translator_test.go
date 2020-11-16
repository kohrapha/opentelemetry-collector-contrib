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
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/translator/conventions"
	"go.opentelemetry.io/collector/translator/internaldata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// Asserts whether dimension sets are equal (i.e. has same sets of dimensions)
func assertDimsEqual(t *testing.T, expected, actual [][]string) {
	// Convert to string for easier sorting
	expectedStringified := make([]string, len(expected))
	actualStringified := make([]string, len(actual))
	for i, v := range expected {
		sort.Strings(v)
		expectedStringified[i] = strings.Join(v, ",")
	}
	for i, v := range actual {
		sort.Strings(v)
		actualStringified[i] = strings.Join(v, ",")
	}
	// Sort across dimension sets for equality checking
	sort.Strings(expectedStringified)
	sort.Strings(actualStringified)
	assert.Equal(t, expectedStringified, actualStringified)
}

func TestTranslateOtToGroupedMetric(t *testing.T) {
	metricsMap := make(map[string]MetricInfo)
	groupedMetricMap := make(map[string]*GroupedMetric)
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}
	md := createMetricTestData()
	rm := internaldata.OCToMetrics(md)
	totalDroppedMetrics := 0
	groupedMetricMap, totalDroppedMetrics = TranslateOtToGroupedMetric(rm, config)

	assert.Equal(t, 1, totalDroppedMetrics)
	assert.NotNil(t, groupedMetricMap)

	metricsMap = groupedMetricMap["NamespaceOTelLibUndefinedfalseisItAnErrormyServiceNS/myServiceNamespanNametestSpan"].Metrics
	assert.Equal(t, 4, len(metricsMap))
	metricsMap = groupedMetricMap["NamespaceOTelLibUndefinedmyServiceNS/myServiceNamespanNametestSpan"].Metrics
	assert.Equal(t, 1, len(metricsMap))

	var labels []string
	for i, _ := range groupedMetricMap["NamespaceOTelLibUndefinedmyServiceNS/myServiceNamespanNametestSpan"].Labels {
		labels = append(labels, i)
	}
	sort.Strings(labels)
	assert.Equal(t, []string{"OTelLib", "spanName"}, labels)
	
	labels = nil
	
	for i, _ := range groupedMetricMap["NamespaceOTelLibUndefinedmyServiceNS/myServiceNamespanNametestSpan"].Labels {
		labels = append(labels, i)
	}
	sort.Strings(labels)
	assert.Equal(t, []string{"OTelLib", "spanName"}, labels)
}

func TestTranslateOtToGroupedMetricWithNameSpace(t *testing.T) {
	metricsMap := make(map[string]MetricInfo)
	groupedMetricMap := make(map[string]*GroupedMetric)
	config := &Config{
		Namespace:             "",
		DimensionRollupOption: ZeroAndSingleDimensionRollup,
	}
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
	rm := internaldata.OCToMetrics(md)
	totalDroppedMetrics := 0
	groupedMetricMap, totalDroppedMetrics = TranslateOtToGroupedMetric(rm, config)
	
	assert.Equal(t, 0, totalDroppedMetrics)
	assert.Equal(t, 0, len(groupedMetricMap))
	
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
		},
	}

	rm = internaldata.OCToMetrics(md)
	groupedMetricMap, totalDroppedMetrics = TranslateOtToGroupedMetric(rm, config)
	metricsMap = groupedMetricMap["NamespaceOTelLibUndefinedfalseisItAnErrormyServiceNSspanNametestSpan"].Metrics

	assert.Equal(t, 0, totalDroppedMetrics)
	assert.NotNil(t, groupedMetricMap)
	assert.Equal(t, 1, len(metricsMap))
	assert.Equal(t, "myServiceNS", groupedMetricMap["NamespaceOTelLibUndefinedfalseisItAnErrormyServiceNSspanNametestSpan"].Namespace)
}

func TestTranslateCWMetricToEMF(t *testing.T) {
	cwMeasurement := CWMeasurement{
		Namespace:  "test-emf",
		Dimensions: [][]string{{"OTelLib"}, {"OTelLib", "spanName"}},
		Metrics: []map[string]string{{
			"Name": "spanCounter",
			"Unit": "Count",
		}},
	}
	timestamp := int64(1596151098037)
	fields := make(map[string]interface{})
	fields["OTelLib"] = "cloudwatch-otel"
	fields["spanName"] = "test"
	fields["spanCounter"] = 0

	met := &CWMetrics{
		Timestamp:    timestamp,
		Fields:       fields,
		Measurements: []CWMeasurement{cwMeasurement},
	}
	inputLogEvent := TranslateCWMetricToEMF([]*CWMetrics{met})

	assert.Equal(t, readFromFile("testdata/testTranslateCWMetricToEMF.json"), *inputLogEvent[0].InputLogEvent.Message, "Expect to be equal")
}

func TestTranslateOtToGroupedMetricWithAllDataTypes(t *testing.T) {
	config := &Config{
		DimensionRollupOption: "",
	}

	testCases := []struct {
		testName string
		metric   *metricspb.Metric
		expected map[string]MetricInfo
	}{
		{
			"Int gauge",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_GAUGE_INT64,
					Unit: "Count",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
			map[string]MetricInfo {
				"foo": MetricInfo{
					Value: int64(1),
					Unit: "Count",
				},
			},
		},
		{
			"Double gauge",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_GAUGE_DOUBLE,
					Unit: "Count",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_DoubleValue{
									DoubleValue: 0.1,
								},
							},
						},
					},
				},
			},
			map[string]MetricInfo {
				"foo": MetricInfo{
					Value: 0.1,
					Unit: "Count",
				},
			},
		},
		{
			"Int sum",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_CUMULATIVE_INT64,
					Unit: "Count",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_Int64Value{
									Int64Value: 1,
								},
							},
						},
					},
				},
			},
			map[string]MetricInfo {
				"foo": MetricInfo{
					Value: 0,
					Unit: "Count",
				},
			},
		},
		{
			"Double sum",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_CUMULATIVE_DOUBLE,
					Unit: "Count",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_DoubleValue{
									DoubleValue: 0.1,
								},
							},
						},
					},
				},
			},
			map[string]MetricInfo {
				"foo": MetricInfo{
					Value: 0,
					Unit: "Count",
				},
			},
		},
		{
			"Double histogram",
			&metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: "foo",
					Type: metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION,
					Unit: "Seconds",
					LabelKeys: []*metricspb.LabelKey{
						{Key: "label1"},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						LabelValues: []*metricspb.LabelValue{
							{Value: "value1", HasValue: true},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
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
					{
						LabelValues: []*metricspb.LabelValue{
							{HasValue: false},
							{Value: "value2", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Value: &metricspb.Point_DistributionValue{
									DistributionValue: &metricspb.DistributionValue{
										Sum:   35.0,
										Count: 18,
										BucketOptions: &metricspb.DistributionValue_BucketOptions{
											Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
												Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
													Bounds: []float64{0, 10},
												},
											},
										},
										Buckets: []*metricspb.DistributionValue_Bucket{
											{
												Count: 5,
											},
											{
												Count: 6,
											},
											{
												Count: 7,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			map[string]MetricInfo {
				"foo": MetricInfo{
					Value: &CWMetricStats{
							Min:   0,
							Max:   10,
							Sum:   15.0,
							Count: 5,
						},
					Unit: "Seconds",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			groupedMetricMap := make(map[string]*GroupedMetric)
			oc := consumerdata.MetricsData{
				Node: &commonpb.Node{},
				Resource: &resourcepb.Resource{
					Labels: map[string]string{
						conventions.AttributeServiceName:      "myServiceName",
						conventions.AttributeServiceNamespace: "myServiceNS",
					},
				},
				Metrics: []*metricspb.Metric{tc.metric},
			}

			// Retrieve *pdata.Metric
			rm := internaldata.OCToMetrics(oc)
			rms := rm.ResourceMetrics()
			assert.Equal(t, 1, rms.Len())
			ilms := rms.At(0).InstrumentationLibraryMetrics()
			assert.Equal(t, 1, ilms.Len())
			metrics := ilms.At(0).Metrics()
			assert.Equal(t, 1, metrics.Len())
			
			totalDroppedMetrics := 0
			groupedMetricMap, totalDroppedMetrics = TranslateOtToGroupedMetric(rm, config)
			key := "NamespaceOTelLibUndefinedlabel1myServiceNS/myServiceNamevalue1"
			
			assert.Equal(t, len(tc.expected), len(groupedMetricMap[key].Metrics))
			assert.Equal(t, 0, totalDroppedMetrics)

			for i, expected := range tc.expected {
				metrics := groupedMetricMap[key].Metrics
				assert.Equal(t, len(tc.expected), len(metrics))
				assert.Equal(t, i, "foo")
				assert.Equal(t, expected.Value, metrics["foo"].Value)
				assert.Equal(t, expected.Unit, metrics["foo"].Unit)
			}
		})
	}

	t.Run("Unhandled metric type", func(t *testing.T) {
		groupedMetricMap := make(map[string]*GroupedMetric)
		md := pdata.NewMetrics()
		rms := md.ResourceMetrics()
		rms.Resize(1)
		rms.At(0).InstrumentationLibraryMetrics().Resize(1)
		rms.At(0).InstrumentationLibraryMetrics().At(0).Metrics().Resize(1)
		metric := rms.At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0)
		metric.InitEmpty()
		metric.SetName("foo")
		metric.SetUnit("Count")
		metric.SetDataType(pdata.MetricDataTypeIntHistogram)

		obs, logs := observer.New(zap.WarnLevel)
		obsConfig := &Config{
			DimensionRollupOption: "",
			logger:                zap.New(obs),
		}
		totalDroppedMetrics := 0
		groupedMetricMap, totalDroppedMetrics = TranslateOtToGroupedMetric(md, obsConfig)
		
		assert.Equal(t, 0, len(groupedMetricMap))
		assert.Equal(t, 0, totalDroppedMetrics)
		
		// Test output warning logs
		expectedLogs := []observer.LoggedEntry{
			{
				Entry: zapcore.Entry{Level: zap.WarnLevel, Message: "Unhandled metric data type."},
				Context: []zapcore.Field{
					zap.String("DataType", "IntHistogram"),
					zap.String("Name", "foo"),
					zap.String("Unit", "Count"),
				},
			},
		}
		assert.Equal(t, 1, logs.Len())
		assert.Equal(t, expectedLogs, logs.AllUntimed())
	})
}

func TestBuildGroupedMetric(t *testing.T) {
	namespace := "Namespace"
	instrLibName := "InstrLibName"
	OTellibDimensionKey := "OTelLib"
	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetName("foo")

	t.Run("Int gauge", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeIntGauge)
		dp := pdata.NewIntDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(int64(-17))

		fields := map[string]interface{}{
			"Namespace": namespace,
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		groupedMetric := buildGroupedMetric(dp, fields, &metric, "")

		assert.NotNil(t, groupedMetric)
		assert.Equal(t, 2, len(groupedMetric.Labels))
		assert.Equal(t, 1, len(groupedMetric.Metrics))
		expectedMetrics := map[string]MetricInfo{
			"foo": MetricInfo {int64(-17), ""},
		}
		assert.Equal(t, expectedMetrics, groupedMetric.Metrics)
		expectedLabels := map[string]string{
			"OTelLib": "InstrLibName",
			"label1": "value1",
		}
		assert.Equal(t, expectedLabels, groupedMetric.Labels)
	})

	t.Run("Double gauge", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeDoubleGauge)
		dp := pdata.NewDoubleDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(0.3)

		fields := map[string]interface{}{
			"Namespace": namespace,
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		groupedMetric := buildGroupedMetric(dp, fields, &metric, "")

		assert.NotNil(t, groupedMetric)
		assert.Equal(t, 2, len(groupedMetric.Labels))
		assert.Equal(t, 1, len(groupedMetric.Metrics))
		expectedMetrics := map[string]MetricInfo{
			"foo": MetricInfo {0.3, ""},
		}
		assert.Equal(t, expectedMetrics, groupedMetric.Metrics)
		expectedLabels := map[string]string{
			"OTelLib": "InstrLibName",
			"label1": "value1",
		}
		assert.Equal(t, expectedLabels, groupedMetric.Labels)
	})

	t.Run("Int sum", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeIntSum)
		metric.IntSum().InitEmpty()
		metric.IntSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
		dp := pdata.NewIntDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(int64(-9))

		fields := map[string]interface{}{
			"Namespace": namespace,
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		groupedMetric := buildGroupedMetric(dp, fields, &metric, "")

		assert.NotNil(t, groupedMetric)
		assert.Equal(t, 2, len(groupedMetric.Labels))
		assert.Equal(t, 1, len(groupedMetric.Metrics))
		expectedMetrics := map[string]MetricInfo{
			"foo": MetricInfo {0, ""},
		}
		assert.Equal(t, expectedMetrics, groupedMetric.Metrics)
		expectedLabels := map[string]string{
			"OTelLib": "InstrLibName",
			"label1": "value1",
		}
		assert.Equal(t, expectedLabels, groupedMetric.Labels)
	})

	t.Run("Double sum", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeDoubleSum)
		metric.DoubleSum().InitEmpty()
		metric.DoubleSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
		dp := pdata.NewDoubleDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetValue(0.3)

		fields := map[string]interface{}{
			"Namespace": namespace,
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		groupedMetric := buildGroupedMetric(dp, fields, &metric, "")

		assert.NotNil(t, groupedMetric)
		assert.Equal(t, 2, len(groupedMetric.Labels))
		assert.Equal(t, 1, len(groupedMetric.Metrics))
		expectedMetrics := map[string]MetricInfo{
			"foo": MetricInfo {0, ""},
		}
		assert.Equal(t, expectedMetrics, groupedMetric.Metrics)
		expectedLabels := map[string]string{
			"OTelLib": "InstrLibName",
			"label1": "value1",
		}
		assert.Equal(t, expectedLabels, groupedMetric.Labels)
	})

	t.Run("Double histogram", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeDoubleHistogram)
		dp := pdata.NewDoubleHistogramDataPoint()
		dp.InitEmpty()
		dp.LabelsMap().InitFromMap(map[string]string{
			"label1": "value1",
		})
		dp.SetCount(uint64(17))
		dp.SetSum(float64(17.13))
		dp.SetBucketCounts([]uint64{1, 2, 3})
		dp.SetExplicitBounds([]float64{1, 2, 3})

		fields := map[string]interface{}{
			"Namespace": namespace,
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		groupedMetric := buildGroupedMetric(dp, fields, &metric, "")

		assert.NotNil(t, groupedMetric)
		assert.Equal(t, 2, len(groupedMetric.Labels))
		assert.Equal(t, 1, len(groupedMetric.Metrics))
		expectedMetrics := map[string]MetricInfo{
			"foo": MetricInfo {&CWMetricStats{
				Min:   1,
				Max:   3,
				Sum:   17.13,
				Count: 17,
			}, ""},
		}
		assert.Equal(t, expectedMetrics, groupedMetric.Metrics)
		expectedLabels := map[string]string{
			"OTelLib": "InstrLibName",
			"label1": "value1",
		}
		assert.Equal(t, expectedLabels, groupedMetric.Labels)
	})	

	t.Run("Invalid datapoint type", func(t *testing.T) {
		metric.SetDataType(pdata.MetricDataTypeIntGauge)
		dp := pdata.NewIntHistogramDataPoint()
		dp.InitEmpty()

		fields := map[string]interface{}{
			"Namespace": namespace,
			OTellibDimensionKey: instrLibName,
			"label1": "value1",
		}

		groupedMetric := buildGroupedMetric(dp, fields, &metric, "")
		assert.Nil(t, groupedMetric)
	})
}

func TestCreateDimensions(t *testing.T) {
	OTelLib := "OTelLib"
	testCases := []struct {
		testName string
		labels   map[string]string
		dims     [][]string
	}{
		{
			"single label",
			map[string]string{"a": "foo"},
			[][]string{
				{"a", OTelLib},
				{OTelLib},
			},
		},
		{
			"multiple labels",
			map[string]string{"a": "foo", "b": "bar"},
			[][]string{
				{"a", "b", OTelLib},
				{OTelLib},
				{OTelLib, "a"},
				{OTelLib, "b"},
			},
		},
		{
			"no labels",
			map[string]string{},
			[][]string{
				{OTelLib},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			dp := pdata.NewIntDataPoint()
			dp.InitEmpty()
			dp.LabelsMap().InitFromMap(tc.labels)
			dimensions, fields := createDimensions(dp, OTelLib, ZeroAndSingleDimensionRollup)

			assertDimsEqual(t, tc.dims, dimensions)

			expectedFields := make(map[string]interface{})
			for k, v := range tc.labels {
				expectedFields[k] = v
			}
			expectedFields[OTellibDimensionKey] = OTelLib

			assert.Equal(t, expectedFields, fields)
		})
	}

}

func TestCalculateRate(t *testing.T) {
	namespace := "Namespace"
	instrLibName := "InstrLibName"
	prevValue := int64(0)
	curValue := int64(10)
	fields := map[string]interface{}{
		"Namespace": namespace,
		"OTelLib": instrLibName,
		"spanName": "test",
		"spanCounter": 0,
		"type": "Int64",
	}
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

func TestDimensionRollup(t *testing.T) {
	testCases := []struct {
		testName               string
		dimensionRollupOption  string
		dims                   []string
		instrumentationLibName string
		expected               [][]string
	}{
		{
			"no rollup w/o instrumentation library name",
			"",
			[]string{"a", "b", "c"},
			noInstrumentationLibraryName,
			nil,
		},
		{
			"no rollup w/ instrumentation library name",
			"",
			[]string{"a", "b", "c"},
			"cloudwatch-otel",
			nil,
		},
		{
			"single dim w/o instrumentation library name",
			SingleDimensionRollupOnly,
			[]string{"a", "b", "c"},
			noInstrumentationLibraryName,
			[][]string{
				{"a"},
				{"b"},
				{"c"},
			},
		},
		{
			"single dim w/ instrumentation library name",
			SingleDimensionRollupOnly,
			[]string{"a", "b", "c"},
			"cloudwatch-otel",
			[][]string{
				{OTellibDimensionKey, "a"},
				{OTellibDimensionKey, "b"},
				{OTellibDimensionKey, "c"},
			},
		},
		{
			"single dim w/o instrumentation library name and only one label",
			SingleDimensionRollupOnly,
			[]string{"a"},
			noInstrumentationLibraryName,
			nil,
		},
		{
			"single dim w/ instrumentation library name and only one label",
			SingleDimensionRollupOnly,
			[]string{"a"},
			"cloudwatch-otel",
			nil,
		},
		{
			"zero + single dim w/o instrumentation library name",
			ZeroAndSingleDimensionRollup,
			[]string{"a", "b", "c"},
			noInstrumentationLibraryName,
			[][]string{
				{},
				{"a"},
				{"b"},
				{"c"},
			},
		},
		{
			"zero + single dim w/ instrumentation library name",
			ZeroAndSingleDimensionRollup,
			[]string{"a", "b", "c"},
			"cloudwatch-otel",
			[][]string{
				{OTellibDimensionKey},
				{OTellibDimensionKey, "a"},
				{OTellibDimensionKey, "b"},
				{OTellibDimensionKey, "c"},
			},
		},
		{
			"zero dim rollup w/o instrumentation library name and no labels",
			ZeroAndSingleDimensionRollup,
			[]string{},
			noInstrumentationLibraryName,
			nil,
		},
		{
			"zero dim rollup w/ instrumentation library name and no labels",
			ZeroAndSingleDimensionRollup,
			[]string{},
			"cloudwatch-otel",
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			rolledUp := dimensionRollup(tc.dimensionRollupOption, tc.dims, tc.instrumentationLibName)
			assert.Equal(t, tc.expected, rolledUp)
		})
	}
}

func readFromFile(filename string) string {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	str := string(data)
	return str
}

func createMetricTestData() consumerdata.MetricsData {
	return consumerdata.MetricsData{
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
}

func TestNeedsCalculateRate(t *testing.T) {
	metric := pdata.NewMetric()
	metric.InitEmpty()
	metric.SetDataType(pdata.MetricDataTypeIntGauge)
	assert.False(t, needsCalculateRate(&metric))
	metric.SetDataType(pdata.MetricDataTypeDoubleGauge)
	assert.False(t, needsCalculateRate(&metric))

	metric.SetDataType(pdata.MetricDataTypeIntHistogram)
	assert.False(t, needsCalculateRate(&metric))
	metric.SetDataType(pdata.MetricDataTypeDoubleHistogram)
	assert.False(t, needsCalculateRate(&metric))

	metric.SetDataType(pdata.MetricDataTypeIntSum)
	metric.IntSum().InitEmpty()
	metric.IntSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
	assert.True(t, needsCalculateRate(&metric))
	metric.IntSum().SetAggregationTemporality(pdata.AggregationTemporalityDelta)
	assert.False(t, needsCalculateRate(&metric))

	metric.SetDataType(pdata.MetricDataTypeDoubleSum)
	metric.DoubleSum().InitEmpty()
	metric.DoubleSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
	assert.True(t, needsCalculateRate(&metric))
	metric.DoubleSum().SetAggregationTemporality(pdata.AggregationTemporalityDelta)
	assert.False(t, needsCalculateRate(&metric))
}

func BenchmarkTranslateCWMetricToEMF(b *testing.B) {
	cwMeasurement := CWMeasurement{
		Namespace:  "test-emf",
		Dimensions: [][]string{{"OTelLib"}, {"OTelLib", "spanName"}},
		Metrics: []map[string]string{{
			"Name": "spanCounter",
			"Unit": "Count",
		}},
	}
	timestamp := int64(1596151098037)
	fields := make(map[string]interface{})
	fields["OTelLib"] = "cloudwatch-otel"
	fields["spanName"] = "test"
	fields["spanCounter"] = 0

	met := &CWMetrics{
		Timestamp:    timestamp,
		Fields:       fields,
		Measurements: []CWMeasurement{cwMeasurement},
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		TranslateCWMetricToEMF([]*CWMetrics{met})
	}
}

func BenchmarkDimensionRollup(b *testing.B) {
	dimensions := []string{"a", "b", "c"}
	for n := 0; n < b.N; n++ {
		dimensionRollup(ZeroAndSingleDimensionRollup, dimensions, "cloudwatch-otel")
	}
}
