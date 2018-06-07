package server

import (
	"github.com/divebomb/exocet/rockredis"
)

type ServerConfig struct {
	// this cluster id is used for server transport to tell
	// different global cluster
	ClusterID            string            `json:"cluster_id"`
	EtcdClusterAddresses string            `json:"etcd_cluster_addresses"`
	BroadcastInterface   string            `json:"broadcast_interface"`
	BroadcastAddr        string            `json:"broadcast_addr"`
	RedisAPIPort         int               `json:"redis_api_port"`
	HttpAPIPort          int               `json:"http_api_port"`
	GrpcAPIPort          int               `json:"grpc_api_port"`
	ProfilePort          int               `json:"profile_port"`
	DataDir              string            `json:"data_dir"`
	DataRsyncModule      string            `json:"data_rsync_module"`
	LocalRaftAddr        string            `json:"local_raft_addr"`
	Tags                 map[string]string `json:"tags"`
	SyncerWriteOnly      bool              `json:"syncer_write_only"`
	SyncerNormalInit     bool              `json:"syncer_normal_init"`
	LearnerRole          string            `json:"learner_role"`
	RemoteSyncCluster    string            `json:"remote_sync_cluster"`
	StateMachineType     string            `json:"state_machine_type"`

	ElectionTick int `json:"election_tick"`
	TickMs       int `json:"tick_ms"`
	// default rocksdb options, can be override by namespace config
	RocksDBOpts rockredis.RockOptions `json:"rocksdb_opts"`
	Namespaces  []NamespaceNodeConfig `json:"namespaces"`
	MaxScanJob  int32                 `json:"max_scan_job"`
}

type NamespaceNodeConfig struct {
	Name           string `json:"name"`
	LocalReplicaID uint64 `json:"local_replica_id"`
}

type ConfigFile struct {
	ServerConf ServerConfig `json:"server_conf"`
}
