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

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter/mapwithexpiry"
)

const (
	CleanInterval = 5 * time.Minute
	MinTimeDiff   = 50 * time.Millisecond // We assume 50 milli-seconds is the minimal gap between two collected data sample to be valid to calculate delta

	OtlibDimensionKey            = "OTLib"
	defaultNameSpace             = "default"
	noInstrumentationLibraryName = "Undefined"

	// See: http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
	maximumLogEventsPerPut = 10000

	// DimensionRollupOptions
	ZeroAndSingleDimensionRollup = "ZeroAndSingleDimensionRollup"
	SingleDimensionRollupOnly    = "SingleDimensionRollupOnly"

	FakeMetricValue = 0
)

var currentState = mapwithexpiry.NewMapWithExpiry(CleanInterval)

type rateState struct {
	value     interface{}
	timestamp int64
}

// CWMetrics defines
type CWMetrics struct {
	Measurements []CwMeasurement
	Timestamp    int64
	Fields       map[string]interface{}
}

type GroupedCWMetric struct {
	Namespace    string
	Metrics      map[string]interface{}
	Timestamp    int64
	Dimensions   map[string]interface{}
	MetricUnits  map[string]string
}

// CwMeasurement defines
type CwMeasurement struct {
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

// TranslateOtToCWMetric converts OT metrics to CloudWatch Metric format
func TranslateOtToCWMetric(rm *pdata.ResourceMetrics, dimensionRollupOption string, cwMetricsMap map[string]*CWMetrics, groupedCWMetricMap map[string]*GroupedCWMetric, namespace string) ([]*CWMetrics, int) {
	var cwMetricLists []*CWMetrics
	totalDroppedMetrics := 0

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

	ilms := rm.InstrumentationLibraryMetrics()
	for j := 0; j < ilms.Len(); j++ {
		ilm := ilms.At(j)
		if ilm.IsNil() {
			continue
		}
		if ilm.InstrumentationLibrary().IsNil() {
			ilm.InstrumentationLibrary().InitEmpty()
			ilm.InstrumentationLibrary().SetName(noInstrumentationLibraryName)
		}
		OTLib := ilm.InstrumentationLibrary().Name()

		metrics := ilm.Metrics()
		for k := 0; k < metrics.Len(); k++ {
			metric := metrics.At(k)
			if metric.IsNil() {
				totalDroppedMetrics++
				continue
			}
			cwMetricList := getMeasurements(&metric, namespace, OTLib, dimensionRollupOption)
			batchCWMetrics(cwMetricList, groupedCWMetricMap, cwMetricsMap)
			cwMetricLists = append(cwMetricLists, cwMetricList...)
		}
	}
	return cwMetricLists, totalDroppedMetrics
}

func batchCWMetrics(cwMetricList []*CWMetrics, groupedCWMetricMap map[string]*GroupedCWMetric, cwMetricsMap map[string]*CWMetrics) {
	for _, met := range cwMetricList {
		if len(met.Measurements[0].Dimensions[0]) > 0 {
			key := strings.Join(met.Measurements[0].Dimensions[0], "")
			if _, ok := cwMetricsMap[key]; ok {
				for _, v := range met.Measurements[0].Metrics {
					groupedCWMetricMap[key].Metrics[v["Name"]] = met.Fields[v["Name"]]
					groupedCWMetricMap[key].MetricUnits[v["Name"]] = v["Unit"]
				}
			} else {
				cwMetricsMap[key]=met
				metricMap := make(map[string]interface{})
				dimensionMap := make(map[string]interface{})
				metricUnits := make(map[string]string)

				for _, v := range met.Measurements[0].Dimensions[0] {
					dimensionMap[v] = met.Fields[v]
				}
				
				for _, v := range met.Measurements[0].Metrics {
					metricMap[v["Name"]] = met.Fields[v["Name"]]
					metricUnits[v["Name"]] = v["Unit"]
				}

				m := &GroupedCWMetric {
					Namespace: met.Measurements[0].Namespace, 
					Metrics: metricMap,
					Timestamp: met.Timestamp, 
					Dimensions: dimensionMap, 
					MetricUnits: metricUnits,
				}

				groupedCWMetricMap[key] = m
			}
		}
	}
}

func TranslateBatchedMetricToEMF(groupedCWMetricMap map[string]*GroupedCWMetric) []*LogEvent {
	// convert CWMetric into map format for compatible with PLE input
	ples := make([]*LogEvent, 0, maximumLogEventsPerPut)
	for _, v := range groupedCWMetricMap {
		fieldMap := make(map[string]interface{})
		dimensionsList := make([][]string, 0)
		metricsList := make([]map[string]string, 0)
		fieldMap["Namespace"] = v.Namespace

		dList := make([]string, 0)
		for key, val := range v.Dimensions {
			dList = append(dList,key)
			fieldMap[key] = val
		} 
		
		dimensionsList = append(dimensionsList, dList)	

		for key, val := range v.MetricUnits {
			metricDef := make(map[string]string)
			metricDef["Name"] = key
			metricDef["Unit"] = val
			metricsList = append(metricsList, metricDef)
			fieldMap[key] = val
		} 
			
		cwm := &CwMeasurement {
			Namespace: v.Namespace,
			Dimensions: dimensionsList,
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

func getMeasurements(metric *pdata.Metric, namespace string, OTLib string, dimensionRollupOption string) []*CWMetrics {
	var result []*CWMetrics

	// metric measure data from OT
	metricMeasure := make(map[string]string)
	// metric measure slice could include multiple metric measures
	metricSlice := []map[string]string{}
	metricMeasure["Name"] = metric.Name()
	metricMeasure["Unit"] = metric.Unit()
	metricSlice = append(metricSlice, metricMeasure)

	switch metric.DataType() {
	case pdata.MetricDataTypeIntGauge:
		dps := metric.IntGauge().DataPoints()
		if dps.Len() == 0 {
			return result
		}
		for m := 0; m < dps.Len(); m++ {
			dp := dps.At(m)
			if dp.IsNil() {
				continue
			}
			cwMetric := buildCWMetricFromDP(dp, metric, namespace, metricSlice, OTLib, dimensionRollupOption)
			if cwMetric != nil {
				result = append(result, cwMetric)
			}
		}
	case pdata.MetricDataTypeDoubleGauge:
		dps := metric.DoubleGauge().DataPoints()
		if dps.Len() == 0 {
			return result
		}
		for m := 0; m < dps.Len(); m++ {
			dp := dps.At(m)
			if dp.IsNil() {
				continue
			}
			cwMetric := buildCWMetricFromDP(dp, metric, namespace, metricSlice, OTLib, dimensionRollupOption)
			if cwMetric != nil {
				result = append(result, cwMetric)
			}
		}
	case pdata.MetricDataTypeIntSum:
		dps := metric.IntSum().DataPoints()
		if dps.Len() == 0 {
			return result
		}
		for m := 0; m < dps.Len(); m++ {
			dp := dps.At(m)
			if dp.IsNil() {
				continue
			}
			cwMetric := buildCWMetricFromDP(dp, metric, namespace, metricSlice, OTLib, dimensionRollupOption)
			if cwMetric != nil {
				result = append(result, cwMetric)
			}
		}
	case pdata.MetricDataTypeDoubleSum:
		dps := metric.DoubleSum().DataPoints()
		if dps.Len() == 0 {
			return result
		}
		for m := 0; m < dps.Len(); m++ {
			dp := dps.At(m)
			if dp.IsNil() {
				continue
			}
			cwMetric := buildCWMetricFromDP(dp, metric, namespace, metricSlice, OTLib, dimensionRollupOption)
			if cwMetric != nil {
				result = append(result, cwMetric)
			}
		}
	case pdata.MetricDataTypeDoubleHistogram:
		dps := metric.DoubleHistogram().DataPoints()
		if dps.Len() == 0 {
			return result
		}
		for m := 0; m < dps.Len(); m++ {
			dp := dps.At(m)
			if dp.IsNil() {
				continue
			}
			cwMetric := buildCWMetricFromHistogram(dp, metric, namespace, metricSlice, OTLib, dimensionRollupOption)
			if cwMetric != nil {
				result = append(result, cwMetric)
			}
		}
	}
	return result
}

func buildCWMetricFromDP(dp interface{}, pmd *pdata.Metric, namespace string, metricSlice []map[string]string, OTLib string, dimensionRollupOption string) *CWMetrics {
	// fields contains metric and dimensions key/value pairs
	fieldsPairs := make(map[string]interface{})
	var dimensionArray [][]string
	// Dimensions Slice
	var dimensionSlice []string
	var dimensionKV pdata.StringMap
	switch metric := dp.(type) {
	case pdata.IntDataPoint:
		dimensionKV = metric.LabelsMap()
	case pdata.DoubleDataPoint:
		dimensionKV = metric.LabelsMap()
	}

	dimensionKV.ForEach(func(k string, v pdata.StringValue) {
		fieldsPairs[k] = v.Value()
		dimensionSlice = append(dimensionSlice, k)
	})
	// add OTLib as an additional dimension
	fieldsPairs[OtlibDimensionKey] = OTLib
	dimensionArray = append(dimensionArray, append(dimensionSlice, OtlibDimensionKey))

	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	var metricVal interface{}
	switch metric := dp.(type) {
	case pdata.IntDataPoint:
		// Put a fake but identical metric value here in order to add metric name into fieldsPairs
		// since calculateRate() needs metric name as one of metric identifiers
		fieldsPairs[pmd.Name()] = int64(FakeMetricValue)
		metricVal = calculateRate(fieldsPairs, metric.Value(), timestamp)
	case pdata.DoubleDataPoint:
		fieldsPairs[pmd.Name()] = float64(FakeMetricValue)
		metricVal = calculateRate(fieldsPairs, metric.Value(), timestamp)
	}
	if metricVal == nil {
		return nil
	}
	fieldsPairs[pmd.Name()] = metricVal

	// EMF dimension attr takes list of list on dimensions. Including single/zero dimension rollup
	rollupDimensionArray := dimensionRollup(dimensionRollupOption, dimensionSlice)
	if len(rollupDimensionArray) > 0 {
		dimensionArray = append(dimensionArray, rollupDimensionArray...)
	}

	cwMeasurement := &CwMeasurement{
		Namespace:  namespace,
		Dimensions: dimensionArray,
		Metrics:    metricSlice,
	}
	metricList := make([]CwMeasurement, 1)
	metricList[0] = *cwMeasurement
	cwMetric := &CWMetrics{
		Measurements: metricList,
		Timestamp:    timestamp,
		Fields:       fieldsPairs,
	}
	return cwMetric
}

func buildCWMetricFromHistogram(metric pdata.DoubleHistogramDataPoint, pmd *pdata.Metric, namespace string, metricSlice []map[string]string, OTLib string, dimensionRollupOption string) *CWMetrics {
	// fields contains metric and dimensions key/value pairs
	fieldsPairs := make(map[string]interface{})
	var dimensionArray [][]string
	// Dimensions Slice
	var dimensionSlice []string
	dimensionKV := metric.LabelsMap()

	dimensionKV.ForEach(func(k string, v pdata.StringValue) {
		fieldsPairs[k] = v.Value()
		dimensionSlice = append(dimensionSlice, k)
	})
	// add OTLib as an additional dimension
	fieldsPairs[OtlibDimensionKey] = OTLib
	dimensionArray = append(dimensionArray, append(dimensionSlice, OtlibDimensionKey))

	timestamp := time.Now().UnixNano() / int64(time.Millisecond)

	bucketBounds := metric.ExplicitBounds()
	metricStats := &CWMetricStats{
		Min:   bucketBounds[0],
		Max:   bucketBounds[len(bucketBounds)-1],
		Count: metric.Count(),
		Sum:   metric.Sum(),
	}
	fieldsPairs[pmd.Name()] = metricStats

	// EMF dimension attr takes list of list on dimensions. Including single/zero dimension rollup
	rollupDimensionArray := dimensionRollup(dimensionRollupOption, dimensionSlice)
	if len(rollupDimensionArray) > 0 {
		dimensionArray = append(dimensionArray, rollupDimensionArray...)
	}

	cwMeasurement := &CwMeasurement{
		Namespace:  namespace,
		Dimensions: dimensionArray,
		Metrics:    metricSlice,
	}
	metricList := make([]CwMeasurement, 1)
	metricList[0] = *cwMeasurement
	cwMetric := &CWMetrics{
		Measurements: metricList,
		Timestamp:    timestamp,
		Fields:       fieldsPairs,
	}
	return cwMetric
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
			deltaVal = val.(float64) - prevStats.value.(float64)
			if deltaTime > MinTimeDiff.Milliseconds() && deltaVal.(float64) >= 0 {
				metricRate = deltaVal.(float64) * 1e3 / float64(deltaTime)
			}
		} else {
			deltaVal = val.(int64) - prevStats.value.(int64)
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

func dimensionRollup(dimensionRollupOption string, originalDimensionSlice []string) [][]string {
	var rollupDimensionArray [][]string
	dimensionZero := []string{OtlibDimensionKey}
	if dimensionRollupOption == ZeroAndSingleDimensionRollup {
		//"Zero" dimension rollup
		if len(originalDimensionSlice) > 0 {
			rollupDimensionArray = append(rollupDimensionArray, dimensionZero)
		}
	}
	if dimensionRollupOption == ZeroAndSingleDimensionRollup || dimensionRollupOption == SingleDimensionRollupOnly {
		//"One" dimension rollup
		for _, dimensionKey := range originalDimensionSlice {
			rollupDimensionArray = append(rollupDimensionArray, append(dimensionZero, dimensionKey))
		}
	}

	return rollupDimensionArray
}
