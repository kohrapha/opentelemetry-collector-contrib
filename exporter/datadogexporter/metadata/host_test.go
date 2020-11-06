// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

<<<<<<< HEAD:exporter/loadbalancingexporter/config_test.go
package loadbalancingexporter

import (
	"path"
=======
package metadata

import (
	"os"
>>>>>>> 3738a5a06e82321318a0380ed27d63f727965a0c:exporter/datadogexporter/metadata/host_test.go
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
<<<<<<< HEAD:exporter/loadbalancingexporter/config_test.go
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configtest"
)

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.ExampleComponents()
	assert.NoError(t, err)

	factories.Exporters[typeStr] = NewFactory()

	cfg, err := configtest.LoadConfigFile(t, path.Join(".", "testdata", "config.yaml"), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)
=======
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/utils/cache"
)

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
>>>>>>> 3738a5a06e82321318a0380ed27d63f727965a0c:exporter/datadogexporter/metadata/host_test.go
}
