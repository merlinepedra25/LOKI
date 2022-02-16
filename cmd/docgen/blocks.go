package main

import (
	"reflect"

	"github.com/weaveworks/common/server"

	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/kv/etcd"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/runtimeconfig"

	"github.com/grafana/loki/pkg/distributor"
	"github.com/grafana/loki/pkg/ingester"
	ingesterclient "github.com/grafana/loki/pkg/ingester/client"
	"github.com/grafana/loki/pkg/loki/common"
	"github.com/grafana/loki/pkg/lokifrontend"
	"github.com/grafana/loki/pkg/querier"
	"github.com/grafana/loki/pkg/querier/queryrange"
	"github.com/grafana/loki/pkg/querier/worker"
	"github.com/grafana/loki/pkg/ruler"
	"github.com/grafana/loki/pkg/scheduler"
	"github.com/grafana/loki/pkg/storage"
	"github.com/grafana/loki/pkg/storage/chunk"
	"github.com/grafana/loki/pkg/storage/chunk/aws"
	"github.com/grafana/loki/pkg/storage/chunk/azure"
	"github.com/grafana/loki/pkg/storage/chunk/gcp"
	"github.com/grafana/loki/pkg/storage/chunk/hedging"
	"github.com/grafana/loki/pkg/storage/chunk/openstack"
	"github.com/grafana/loki/pkg/storage/stores/shipper/compactor"
	"github.com/grafana/loki/pkg/tracing"
	"github.com/grafana/loki/pkg/usagestats"
	"github.com/grafana/loki/pkg/validation"
)

var (
	rootBlocks = []rootBlock{
		{
			name:       "common_config",
			structType: reflect.TypeOf(common.Config{}),
			desc:       "Common configuration shared between multiple modules.\nIf a more specific configuration is given in other sections, the related configuration within thie section will be ignored.",
		},
		{
			name:       "server_config",
			structType: reflect.TypeOf(server.Config{}),
			desc:       "Configures the HTTP/gRPC server of the started module(s).",
		},
		{
			name:       "distributor_config",
			structType: reflect.TypeOf(distributor.Config{}),
			desc:       "Configures the distributors and how they connect to each other via the ring.",
		},
		{
			name:       "querier_config",
			structType: reflect.TypeOf(querier.Config{}),
			desc:       "Configures the queriers\nOnly applicable when running target `all` or `querier`.",
		},
		{
			name:       "ingester_config",
			structType: reflect.TypeOf(ingester.Config{}),
			desc:       "Configures the ingesters and how they register themselves to the distributor ring.",
		},
		{
			name:       "ingester_client_config",
			structType: reflect.TypeOf(ingesterclient.Config{}),
			desc:       "Configures how the ingester clients on the distributors and queriers will connect to the ingesters\nOnly applicable when running target `all`, `distributor`, or `querier`.",
		},
		{
			name:       "storage_config",
			structType: reflect.TypeOf(storage.Config{}),
			desc:       "Configures where Loki stores data.",
		},
		{
			name:       "chunkstore_config",
			structType: reflect.TypeOf(storage.ChunkStoreConfig{}),
			desc:       "Configures how Loki stores data in the specific store.",
		},
		{
			name:       "schema_config",
			structType: reflect.TypeOf(storage.SchemaConfig{}),
			desc:       "Configures the chunk index schema and where it is stored.",
		},
		{
			name:       "limits_config",
			structType: reflect.TypeOf(validation.Limits{}),
			desc:       "Configures per-tenant or global limits.",
		},
		{
			name:       "table_manager_config",
			structType: reflect.TypeOf(chunk.TableManagerConfig{}),
			desc:       "Configures how long the table manager retains data.",
		},
		{
			name:       "frontend_worker_config",
			structType: reflect.TypeOf(worker.Config{}),
			desc:       "Configures the workers that are running with in the queriers and picking up and executing queries enqued by the query frontend.",
		},
		{
			name:       "frontend_config",
			structType: reflect.TypeOf(lokifrontend.Config{}),
			desc:       "Configures the query frontends.",
		},
		{
			name:       "ruler_config",
			structType: reflect.TypeOf(ruler.Config{}),
			desc:       "Configures the rulers.",
		},
		{
			name:       "query_range_config",
			structType: reflect.TypeOf(queryrange.Config{}),
			desc:       "Configures how the query frontend splits and caches queries.",
		},
		{
			name:       "runtime_config",
			structType: reflect.TypeOf(runtimeconfig.Config{}),
			desc:       "Configures the runtime config file and how often it is reloaded.",
		},
		{
			name:       "memberlist_config",
			structType: reflect.TypeOf(memberlist.KVConfig{}),
			desc:       "Configures how ring members communicate with each other using memberlist.",
		},
		{
			name:       "tracing_config",
			structType: reflect.TypeOf(tracing.Config{}),
			desc:       "Configures tracing options.",
		},
		{
			name:       "compactor_config",
			structType: reflect.TypeOf(compactor.Config{}),
			desc:       "Configures how the compactors compact index shards for performance.",
		},
		{
			name:       "query_scheduler_config",
			structType: reflect.TypeOf(scheduler.Config{}),
			desc:       "Configures the query schedulers.\nWhen configured, tenant query queues are separated from the query frontend.",
		},
		{
			name:       "analytics_config",
			structType: reflect.TypeOf(usagestats.Config{}),
			desc:       "Configures how anonymous usage data is sent to grafana.com.",
		},

		// common configuration blocks

		{
			name:       "consul_config",
			structType: reflect.TypeOf(consul.Config{}),
			desc:       "Configures the Consul client.",
		},
		{
			name:       "etcd_config",
			structType: reflect.TypeOf(etcd.Config{}),
			desc:       "Configures the etcd client.",
		},

		// common storage blocks

		{
			name:       "azure_storage_config",
			structType: reflect.TypeOf(azure.BlobStorageConfig{}),
			desc:       "Configures the client for Azure Blob Storage as storage.",
		},
		{
			name:       "gcs_storage_config",
			structType: reflect.TypeOf(gcp.GCSConfig{}),
			desc:       "Configures the client for GCS as storage.",
		},
		{
			name:       "s3_storage_config",
			structType: reflect.TypeOf(aws.S3Config{}),
			desc:       "Configures the client Amazon S3 as storage",
		},
		{
			name:       "swift_storage_config",
			structType: reflect.TypeOf(openstack.SwiftConfig{}),
			desc:       "Configures Swift as storage",
		},
		{
			name:       "filesystem_storage_config",
			structType: reflect.TypeOf(common.FilesystemConfig{}),
			desc:       "Configures a (local) file system as storage",
		},
		{
			name:       "hedging_config",
			structType: reflect.TypeOf(hedging.Config{}),
			desc:       "Configures how to hedge requests for the storage",
		},
	}
)
