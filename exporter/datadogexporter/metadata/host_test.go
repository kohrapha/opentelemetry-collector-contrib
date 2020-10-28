// Copyright The OpenTelemetry Authors
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

<<<<<<< HEAD:receiver/zookeeperreceiver/scraper.go
package zookeeperreceiver

import (
	"context"

	"go.opentelemetry.io/collector/consumer/pdata"
=======
package metadata

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
>>>>>>> b0609985c15cf19d79ac5851f4b9fd70caacebee:exporter/datadogexporter/metadata/host_test.go
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/utils/cache"
)

<<<<<<< HEAD:receiver/zookeeperreceiver/scraper.go
type zookeeperMetricsScraper struct {
}

func newZookeeperMetricsScraper(logger *zap.Logger, config *Config) (*zookeeperMetricsScraper, error) {
	return &zookeeperMetricsScraper{}, nil
}

func (z *zookeeperMetricsScraper) Initialize(_ context.Context) error {
	return nil
}

func (z *zookeeperMetricsScraper) Close(_ context.Context) error {
	return nil
}

func (z *zookeeperMetricsScraper) Scrape(_ context.Context) (pdata.ResourceMetricsSlice, error) {
	return pdata.ResourceMetricsSlice{}, nil
=======
func TestHost(t *testing.T) {

	logger := zap.NewNop()

	host := GetHost(logger, &config.Config{
		TagsConfig: config.TagsConfig{Hostname: "test-host"},
	})
	assert.Equal(t, *host, "test-host")
	cache.Cache.Delete(cache.CanonicalHostnameKey)

	host = GetHost(logger, &config.Config{})
	osHostname, err := os.Hostname()
	require.NoError(t, err)
	assert.Equal(t, *host, osHostname)
>>>>>>> b0609985c15cf19d79ac5851f4b9fd70caacebee:exporter/datadogexporter/metadata/host_test.go
}
