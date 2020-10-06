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

// Characterizes a rule to be used to set dimensions for certain incoming metrics,
// filtered by their metric names. 
type MetricDeclaration struct {
	// List of dimension sets (which are lists of dimension names) to be included
	// in exported metrics. If the metric does not contain any of the specified
	// dimensions, the metric would be dropped (will only show up in logs).
	Dimensions 			[][]string `mapstructure:"dimensions"`
	// List of regex strings to be matched against metric names to determine which
	// metrics should be included with this metric declaration rule.
	MetricNameSelectors []string `mapstructure:"metric_name_selectors"`
}


