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
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"sort"
	"time"
	"strings"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/translator/conventions"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter/mapwithexpiry"
)

const (
	CleanInterval = 5 * time.Minute
	MinTimeDiff   = 50 * time.Millisecond // We assume 50 milli-seconds is the minimal gap between two collected data sample to be valid to calculate delta

	// OTel instrumentation lib name as dimension
	OTellibDimensionKey          = "OTelLib"
	defaultNameSpace             = "default"
	noInstrumentationLibraryName = "Undefined"

	// See: http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
	maximumLogEventsPerPut = 10000

	// DimensionRollupOptions
	ZeroAndSingleDimensionRollup = "ZeroAndSingleDimensionRollup"
	SingleDimensionRollupOnly    = "SingleDimensionRollupOnly"
)

var currentState = mapwithexpiry.NewMapWithExpiry(CleanInterval)

type rateState struct {
	value     interface{}
	timestamp int64
}

// MetricInfo defines
type MetricInfo struct {
	Value  interface{}
	Unit   string
}

// GroupedMetric defines
type GroupedMetric struct {
	Namespace    string
	Timestamp    int64
	Labels      map[string]interface{}
	Metrics     map[string]MetricInfo
}

// CWMetrics defines
type CWMetrics struct {
	Measurements []CWMeasurement
	Timestamp    int64
	Fields       map[string]interface{}
}

// CWMeasurement defines
type CWMeasurement struct {
	Namespace  string
	Dimensions [][]string
	Metrics    []map[string]string
}

// CWMetric stats defines
type CWMetricStats struct {
	Max   float64
	Min   float64
	Count uint64
	Sum   float64
}

// Wrapper interface for:
// 	- pdata.IntDataPointSlice
// 	- pdata.DoubleDataPointSlice
// 	- pdata.IntHistogramDataPointSlice
// 	- pdata.DoubleHistogramDataPointSlice
type DataPoints interface {
	Len() int
	At(int) DataPoint
}

// Wrapper interface for:
// 	- pdata.IntDataPoint
// 	- pdata.DoubleDataPoint
// 	- pdata.IntHistogramDataPoint
// 	- pdata.DoubleHistogramDataPoint
type DataPoint interface {
	IsNil() bool
	LabelsMap() pdata.StringMap
}

// Define wrapper interfaces such that At(i) returns a `DataPoint`
type IntDataPointSlice struct {
	pdata.IntDataPointSlice
}
type DoubleDataPointSlice struct {
	pdata.DoubleDataPointSlice
}
type DoubleHistogramDataPointSlice struct {
	pdata.DoubleHistogramDataPointSlice
}

func (dps IntDataPointSlice) At(i int) DataPoint {
	return dps.IntDataPointSlice.At(i)
}
func (dps DoubleDataPointSlice) At(i int) DataPoint {
	return dps.DoubleDataPointSlice.At(i)
}
func (dps DoubleHistogramDataPointSlice) At(i int) DataPoint {
	return dps.DoubleHistogramDataPointSlice.At(i)
}


// Validates namespace for given set of metrics from user config 
func validateNamespace(rm *pdata.ResourceMetrics, namespace string) (string) {
	if len(namespace) == 0 && !rm.Resource().IsNil() {
		serviceName, svcNameOk := rm.Resource().Attributes().Get(conventions.AttributeServiceName)
		serviceNamespace, svcNsOk := rm.Resource().Attributes().Get(conventions.AttributeServiceNamespace)
		if svcNameOk && svcNsOk && serviceName.Type() == pdata.AttributeValueSTRING && serviceNamespace.Type() == pdata.AttributeValueSTRING {
			namespace = fmt.Sprintf("%s/%s", serviceNamespace.StringVal(), serviceName.StringVal())
		} else if svcNameOk && serviceName.Type() == pdata.AttributeValueSTRING {
			namespace = serviceName.StringVal()
		} else if svcNsOk && serviceNamespace.Type() == pdata.AttributeValueSTRING {
			namespace = serviceNamespace.StringVal()
		}
	}

	if len(namespace) == 0 {
		namespace = defaultNameSpace
	}
	return namespace
}

// TranslateOtToGroupedMetric converts OT metrics to GroupedMetric format
func TranslateOtToGroupedMetric(metric pdata.Metrics, groupedMetricMap map[string]*GroupedMetric, config *Config) (int) {
	totalDroppedMetrics := 0

	var dps DataPoints
	var instrumentationLibName string
	rms := metric.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		if rm.IsNil() {
			continue
		}
		namespace := validateNamespace(&rm, config.Namespace)
		ilms := rm.InstrumentationLibraryMetrics()
		for j := 0; j < ilms.Len(); j++ {
			ilm := ilms.At(j)
			if ilm.IsNil() {
				continue
			}

			if ilm.InstrumentationLibrary().IsNil() {
				instrumentationLibName = noInstrumentationLibraryName
			} else {
				instrumentationLibName = ilm.InstrumentationLibrary().Name()
			}
			
			metrics := ilm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				if metric.IsNil() {
					totalDroppedMetrics++
					continue
				}

				switch metric.DataType() {
					case pdata.MetricDataTypeIntGauge:
						dps = IntDataPointSlice{metric.IntGauge().DataPoints()}
					case pdata.MetricDataTypeDoubleGauge:
						dps = DoubleDataPointSlice{metric.DoubleGauge().DataPoints()}
					case pdata.MetricDataTypeIntSum:
						dps = IntDataPointSlice{metric.IntSum().DataPoints()}
					case pdata.MetricDataTypeDoubleSum:
						dps = DoubleDataPointSlice{metric.DoubleSum().DataPoints()}
					case pdata.MetricDataTypeDoubleHistogram:
						dps = DoubleHistogramDataPointSlice{metric.DoubleHistogram().DataPoints()}
					default:
						config.logger.Warn("Unhandled metric data type.",
							zap.String("DataType", metric.DataType().String()),
							zap.String("Name", metric.Name()),
							zap.String("Unit", metric.Unit()),)
						return totalDroppedMetrics
				}

				if dps.Len() == 0 {
					continue
				}

				for m := 0; m < dps.Len(); m++ {
					dp := dps.At(m)
					if dp.IsNil() {
						continue
					}

					labelSlice := make([]string, 0, dp.LabelsMap().Len())
					dp.LabelsMap().ForEach(func(k string, v string) {
						labelSlice = append(labelSlice, k)
					})
					sort.Strings(labelSlice)
					key := strings.Join(labelSlice, "")

					if _, ok := groupedMetricMap[key]; ok {
						updateGroupedMetric(dp, &metric, key, groupedMetricMap)
					} else {
						groupedMetric := buildGroupedMetric(dp, &metric, instrumentationLibName, namespace)
						groupedMetricMap[key] = groupedMetric
					}
				}
			}
		}
	}
	return totalDroppedMetrics
}

// convert map of GroupedMetric objects into map format for compatible with PLE input
func TranslateBatchedMetricToEMF(groupedMetricMap map[string]*GroupedMetric) []*LogEvent {
	ples := make([]*LogEvent, 0, maximumLogEventsPerPut)
	for _, v := range groupedMetricMap {
		fieldMap := make(map[string]interface{})
		labelsList := make([][]string, 0)
		metricsList := make([]map[string]string, 0)
		fieldMap["Namespace"] = v.Namespace

		lList := make([]string, 0)
		for key, val := range v.Labels {
			lList = append(lList,key)
			fieldMap[key] = val
		}

		labelsList = append(labelsList, lList)

		for key, val := range v.Metrics {
			metricDef := make(map[string]string)
			metricDef["Name"] = key
			metricDef["Unit"] = val.Unit
			metricsList = append(metricsList, metricDef)
			fieldMap[key] = val.Value
		}

		cwm := &CWMeasurement {
			Namespace: v.Namespace,
			Dimensions: labelsList,
			Metrics: metricsList,
		}

		cwmMap := make(map[string]interface{})
		cwmMap["CloudWatchMetrics"] = cwm
		cwmMap["Timestamp"] = v.Timestamp
		fieldMap["_aws"] = cwmMap

		pleMsg, err := json.Marshal(fieldMap)
		if err != nil {
			continue
		}
		metricCreationTime := v.Timestamp

		logEvent := NewLogEvent(
			metricCreationTime,
			string(pleMsg),
		)
		logEvent.LogGeneratedTime = time.Unix(0, metricCreationTime*int64(time.Millisecond))
		ples = append(ples, logEvent)
	}
	return ples
}

func TranslateCWMetricToEMF(cwMetricLists []*CWMetrics) []*LogEvent {
	// convert CWMetric into map format for compatible with PLE input
	ples := make([]*LogEvent, 0, maximumLogEventsPerPut)
	for _, met := range cwMetricLists {
		cwmMap := make(map[string]interface{})
		fieldMap := met.Fields
		cwmMap["CloudWatchMetrics"] = met.Measurements
		cwmMap["Timestamp"] = met.Timestamp
		fieldMap["_aws"] = cwmMap

		pleMsg, err := json.Marshal(fieldMap)
		if err != nil {
			continue
		}
		metricCreationTime := met.Timestamp

		logEvent := NewLogEvent(
			metricCreationTime,
			string(pleMsg),
		)
		logEvent.LogGeneratedTime = time.Unix(0, metricCreationTime*int64(time.Millisecond))
		ples = append(ples, logEvent)
	}
	return ples
}

// Build grouped metric from Datapoint and pdata.Metric
func buildGroupedMetric (dp DataPoint, pMetricData *pdata.Metric, instrumentationLibName string, namespace string) *GroupedMetric {
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	labels := make(map[string]interface{})
	metrics := make(map[string]MetricInfo)
	// Extract labels
	labelMap := dp.LabelsMap()
	// v pdata.StringValue?
	// v.Value()?
	labelMap.ForEach(func(k string, v string) {
		labels[k] = v
	})
	// Extract metric
	var metricVal interface{}
	switch metric := dp.(type) {
	case pdata.IntDataPoint:
		metricVal = int64(metric.Value())
		if needsCalculateRate(pMetricData) {
			metricVal = calculateRate(labels, metric.Value(), timestamp)
		}
	case pdata.DoubleDataPoint:
		metricVal = float64(metric.Value())
		if needsCalculateRate(pMetricData) {
			metricVal = calculateRate(labels, metric.Value(), timestamp)
		}
	case pdata.DoubleHistogramDataPoint:
		bucketBounds := metric.ExplicitBounds()
		metricVal = &CWMetricStats{
			Min:   bucketBounds[0],
			Max:   bucketBounds[len(bucketBounds)-1],
			Count: metric.Count(),
			Sum:   metric.Sum(),
		}
	}
	if metricVal == nil {
		return nil
	}
	metricInfo := &MetricInfo {
		Value: metricVal,
		Unit:  pMetricData.Unit(),
	}

	metrics[pMetricData.Name()] = *metricInfo

	groupedMetric := &GroupedMetric {
		Namespace: namespace,
		Timestamp: timestamp,
		Labels:    labels,
		Metrics:   metrics,
	}
	return groupedMetric
}

// Update GroupedMetric with new metric from datapoint
func updateGroupedMetric (dp DataPoint, pMetricData *pdata.Metric, key string, groupedMetricMap map[string]*GroupedMetric) {
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)

	// Extract metric
	var metricVal interface{}
	switch metric := dp.(type) {
	case pdata.IntDataPoint:
		metricVal = int64(metric.Value())

	case pdata.DoubleDataPoint:
		metricVal = float64(metric.Value())

	case pdata.DoubleHistogramDataPoint:
		bucketBounds := metric.ExplicitBounds()
		metricVal = &CWMetricStats{
			Min:   bucketBounds[0],
			Max:   bucketBounds[len(bucketBounds)-1],
			Count: metric.Count(),
			Sum:   metric.Sum(),
		}
	}

	if metricVal == nil {
		return
	}
	metricInfo := &MetricInfo {
		Value: metricVal,
		Unit:  pMetricData.Unit(),
	}

	groupedMetricMap[key].Metrics[pMetricData.Name()] = *metricInfo
	groupedMetricMap[key].Timestamp = timestamp
}

// Create dimensions from DataPoint labels, where dimensions is a 2D array of dimension names,
// and initialize fields with dimension key/value pairs
func createDimensions(dp DataPoint, instrumentationLibName string, dimensionRollupOption string) (dimensions [][]string, fields map[string]interface{}) {
	// fields contains metric and dimensions key/value pairs
	fields = make(map[string]interface{})
	dimensionKV := dp.LabelsMap()

	dimensionSlice := make([]string, dimensionKV.Len(), dimensionKV.Len()+1)
	idx := 0
	dimensionKV.ForEach(func(k string, v string) {
		fields[k] = v
		dimensionSlice[idx] = k
		idx++
	})
	// Add OTel instrumentation lib name as an additional dimension if it is defined
	if instrumentationLibName != noInstrumentationLibraryName {
		fields[OTellibDimensionKey] = instrumentationLibName
		dimensions = append(dimensions, append(dimensionSlice, OTellibDimensionKey))
	} else {
		dimensions = append(dimensions, dimensionSlice)
	}

	// EMF dimension attr takes list of list on dimensions. Including single/zero dimension rollup
	rollupDimensionArray := dimensionRollup(dimensionRollupOption, dimensionSlice, instrumentationLibName)
	if len(rollupDimensionArray) > 0 {
		dimensions = append(dimensions, rollupDimensionArray...)
	}

	return
}

// rate is calculated by valDelta / timeDelta
func calculateRate(fields map[string]interface{}, val interface{}, timestamp int64) interface{} {
	keys := make([]string, 0, len(fields))
	var b bytes.Buffer
	var metricRate interface{}
	// hash the key of str: metric + dimension key/value pairs (sorted alpha)
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		switch v := fields[k].(type) {
		case int64:
			b.WriteString(k)
			continue
		case string:
			b.WriteString(k)
			b.WriteString(v)
		default:
			continue
		}
	}
	h := sha1.New()
	h.Write(b.Bytes())
	bs := h.Sum(nil)
	hashStr := string(bs)

	// get previous Metric content from map. Need to lock the map until set the new state
	currentState.Lock()
	if state, ok := currentState.Get(hashStr); ok {
		prevStats := state.(*rateState)
		deltaTime := timestamp - prevStats.timestamp
		var deltaVal interface{}

		if _, ok := val.(float64); ok {
			if _, ok := prevStats.value.(int64); ok {
				deltaVal = val.(float64) - float64(prevStats.value.(int64))
			} else {
				deltaVal = val.(float64) - prevStats.value.(float64)
			}
			if deltaTime > MinTimeDiff.Milliseconds() && deltaVal.(float64) >= 0 {
				metricRate = deltaVal.(float64) * 1e3 / float64(deltaTime)
			}
		} else {
			if _, ok := prevStats.value.(float64); ok {
				deltaVal = val.(int64) - int64(prevStats.value.(float64))
			} else {
				deltaVal = val.(int64) - prevStats.value.(int64)
			}
			if deltaTime > MinTimeDiff.Milliseconds() && deltaVal.(int64) >= 0 {
				metricRate = deltaVal.(int64) * 1e3 / deltaTime
			}
		}
	}
	content := &rateState{
		value:     val,
		timestamp: timestamp,
	}
	currentState.Set(hashStr, content)
	currentState.Unlock()
	if metricRate == nil {
		metricRate = 0
	}
	return metricRate
}

func dimensionRollup(dimensionRollupOption string, originalDimensionSlice []string, instrumentationLibName string) [][]string {
	var rollupDimensionArray [][]string
	dimensionZero := []string{}
	if instrumentationLibName != noInstrumentationLibraryName {
		dimensionZero = append(dimensionZero, OTellibDimensionKey)
	}
	if dimensionRollupOption == ZeroAndSingleDimensionRollup {
		//"Zero" dimension rollup
		if len(originalDimensionSlice) > 0 {
			rollupDimensionArray = append(rollupDimensionArray, dimensionZero)
		}
	}
	if dimensionRollupOption == ZeroAndSingleDimensionRollup || dimensionRollupOption == SingleDimensionRollupOnly {
		//"One" dimension rollup
		if len(originalDimensionSlice) > 1 {
			for _, dimensionKey := range originalDimensionSlice {
				rollupDimensionArray = append(rollupDimensionArray, append(dimensionZero, dimensionKey))
			}
		}
	}

	return rollupDimensionArray
}

func needsCalculateRate(pmd *pdata.Metric) bool {
	switch pmd.DataType() {
	case pdata.MetricDataTypeIntSum:
		if !pmd.IntSum().IsNil() && pmd.IntSum().AggregationTemporality() == pdata.AggregationTemporalityCumulative {
			return true
		}
	case pdata.MetricDataTypeDoubleSum:
		if !pmd.DoubleSum().IsNil() && pmd.DoubleSum().AggregationTemporality() == pdata.AggregationTemporalityCumulative {
			return true
		}
	}
	return false
}
