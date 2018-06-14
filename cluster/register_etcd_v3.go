package cluster

import (
	"encoding/json"
	"errors"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/divebomb/exocet/common"
	"golang.org/x/net/context"
)

const (
	EVENT_WATCH_L_CREATE = iota
	EVENT_WATCH_L_DELETE
)

const (
	ETCD_TTL = 60

	etcdDialTimeout   = time.Second * 2
	kvRequestTimeout  = time.Second * 10
	kvSlowRequestTime = time.Second * 1
)

const (
	ROOT_DIR               = "ZanRedisDBMetaData"
	CLUSTER_META_INFO      = "ClusterMeta"
	NAMESPACE_DIR          = "Namespaces"
	NAMESPACE_META         = "NamespaceMeta"
	NAMESPACE_SCHEMA       = "NamespaceSchema"
	NAMESPACE_REPLICA_INFO = "ReplicaInfo"
	NAMESPACE_REAL_LEADER  = "RealLeader"
	DATA_NODE_DIR          = "DataNodes"
	PD_ROOT_DIR            = "PDInfo"
	PD_NODE_DIR            = "PDNodes"
	PD_LEADER_SESSION      = "PDLeaderSession"
)

const (
	ETCD_LOCK_NAMESPACE = "zanredisdb"
)

var ErrNoKV = errors.New("no any key and value")

type MasterChanInfo struct {
	processStopCh chan bool
	stoppedCh     chan bool
}

func exchangeNodeValue(c *clientv3.Client, nodePath string, initValue string,
	valueChangeFn func(bool, string) (string, error)) error {
	var oldValue string
	isNew := false
	t1 := c.Txn(context.Background()).If(clientv3.Compare(clientv3.CreateRevision(nodePath), "=", 0))
	t1 = t1.Then(clientv3.OpPut(nodePath, initValue))
	t1 = t1.Else(clientv3.OpGet(nodePath))
	resp1, err := t1.Commit()
	if err != nil {
		return err
	}
	if resp1.Succeeded {
		isNew = true
		oldValue = initValue
	} else {
		kv := resp1.Responses[0].GetResponseRange().Kvs[0]
		oldValue = string(kv.Value)
	}
	var newValue string
	retry := 5
	for retry > 0 {
		retry--
		newValue, err = valueChangeFn(isNew, oldValue)
		if err != nil {
			return err
		}

		t2 := c.Txn(context.Background()).If(clientv3.Compare(clientv3.Value(nodePath), "=", oldValue))
		t2 = t2.Then(clientv3.OpPut(nodePath, newValue))
		t2 = t2.Else(clientv3.OpGet(nodePath))
		resp2, err := t2.Commit()
		if err != nil {
			return err
		}
		if resp2.Succeeded {
			return nil
		}
		kv := resp2.Responses[0].GetResponseRange().Kvs[0]
		oldValue = string(kv.Value)
		isNew = false
		time.Sleep(time.Millisecond * 10)
	}
	return err
}

type EtcdRegister struct {
	nsMutex sync.Mutex

	client               *clientv3.Client
	clusterID            string
	namespaceRoot        string
	clusterPath          string
	pdNodeRootPath       string
	allNamespaceInfos    map[string]map[int]PartitionMetaInfo
	nsEpoch              EpochType
	ifNamespaceChanged   int32
	watchNamespaceStopCh chan struct{}
	nsChangedChan        chan struct{}
	triggerScanCh        chan struct{}
	wg                   sync.WaitGroup
}

//[finish]
func NewEtcdRegister(host string) *EtcdRegister {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(host, ","),
		DialTimeout: etcdDialTimeout,
	})
	if err != nil {
		coordLog.Errorf("create etcd client v3 with endpoints[%s] error: %s", host, err.Error())
		return nil
	}
	r := &EtcdRegister{
		allNamespaceInfos:    make(map[string]map[int]PartitionMetaInfo),
		watchNamespaceStopCh: make(chan struct{}),
		client:               client,
		ifNamespaceChanged:   1,
		nsChangedChan:        make(chan struct{}, 3),
		triggerScanCh:        make(chan struct{}, 3),
	}
	return r
}

func (etcdReg *EtcdRegister) InitClusterID(id string) {
	etcdReg.clusterID = id
	etcdReg.namespaceRoot = etcdReg.getNamespaceRootPath()
	etcdReg.clusterPath = etcdReg.getClusterPath()
	etcdReg.pdNodeRootPath = etcdReg.getPDNodeRootPath()
}

func (etcdReg *EtcdRegister) Start() {
	etcdReg.watchNamespaceStopCh = make(chan struct{})
	etcdReg.wg.Add(2)
	go func() {
		defer etcdReg.wg.Done()
		etcdReg.watchNamespaces(etcdReg.watchNamespaceStopCh)
	}()
	go func() {
		defer etcdReg.wg.Done()
		etcdReg.refreshNamespaces(etcdReg.watchNamespaceStopCh)
	}()
}

func (etcdReg *EtcdRegister) Stop() {
	coordLog.Infof("stopping etcd register")
	defer coordLog.Infof("stopped etcd register")
	if etcdReg.watchNamespaceStopCh != nil {
		select {
		case <-etcdReg.watchNamespaceStopCh:
		default:
			close(etcdReg.watchNamespaceStopCh)
		}
	}
	etcdReg.wg.Wait()
}

//[finish]
func (etcdReg *EtcdRegister) GetAllPDNodes() ([]NodeInfo, error) {
	resp, err := kvGet(etcdReg.client, etcdReg.pdNodeRootPath)
	if err != nil {
		return nil, err
	}
	nodeList := make([]NodeInfo, 0)
	for _, node := range resp.Kvs {
		var nodeInfo NodeInfo
		if err = json.Unmarshal([]byte(node.Value), &nodeInfo); err != nil {
			continue
		}
		nodeList = append(nodeList, nodeInfo)
	}
	return nodeList, nil
}

func (etcdReg *EtcdRegister) GetAllNamespaces() (map[string]map[int]PartitionMetaInfo, EpochType, error) {
	if atomic.LoadInt32(&etcdReg.ifNamespaceChanged) == 1 {
		return etcdReg.scanNamespaces()
	}

	etcdReg.nsMutex.Lock()
	nsInfos := etcdReg.allNamespaceInfos
	nsEpoch := etcdReg.nsEpoch
	etcdReg.nsMutex.Unlock()
	return nsInfos, nsEpoch, nil
}

//[finish]
func (etcdReg *EtcdRegister) GetNamespaceSchemas(ns string) (map[string]SchemaInfo, error) {
	resp, err := kvGet(etcdReg.client, etcdReg.getNamespaceSchemaPath(ns))
	if err != nil {
		return nil, err
	}

	schemas := make(map[string]SchemaInfo)
	for _, node := range resp.Kvs {
		_, table := path.Split(string(node.Key))
		var sInfo SchemaInfo
		sInfo.Schema = node.Value
		sInfo.Epoch = EpochType(node.ModRevision)
		schemas[table] = sInfo
	}

	return schemas, nil
}

func (etcdReg *EtcdRegister) GetNamespacesNotifyChan() chan struct{} {
	return etcdReg.nsChangedChan
}

func (etcdReg *EtcdRegister) refreshNamespaces(stopC <-chan struct{}) {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()
	for {
		select {
		case <-stopC:
			return
		case <-etcdReg.triggerScanCh:
			if atomic.LoadInt32(&etcdReg.ifNamespaceChanged) == 1 {
				etcdReg.scanNamespaces()
			}
		case <-ticker.C:
			if atomic.LoadInt32(&etcdReg.ifNamespaceChanged) == 1 {
				etcdReg.scanNamespaces()
			}
		}
	}
}

//[finish]
func (etcdReg *EtcdRegister) watchNamespaces(stopC <-chan struct{}) {
	watcher := createWatch(etcdReg.client, etcdReg.namespaceRoot)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stopC:
			cancel()
		}
	}()

	for {
		_, err := watcher.WaitNext(ctx, 0)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", etcdReg.namespaceRoot)
				return
			}
		}
		coordLog.Debugf("namespace changed.")
		atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
		select {
		case etcdReg.triggerScanCh <- struct{}{}:
		default:
		}
		select {
		case etcdReg.nsChangedChan <- struct{}{}:
		default:
		}
	}
}

func (etcdReg *EtcdRegister) scanNamespaces() (map[string]map[int]PartitionMetaInfo, EpochType, error) {
	coordLog.Infof("refreshing namespaces")
	atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 0)

	resp, err := kvGet(etcdReg.client, etcdReg.namespaceRoot)
	if err != nil {
		atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
		etcdReg.nsMutex.Lock()
		nsInfos := etcdReg.allNamespaceInfos
		nsEpoch := etcdReg.nsEpoch
		etcdReg.nsMutex.Unlock()
		coordLog.Infof("refreshing namespaces failed: %v, use old info instead", err)
		return nsInfos, nsEpoch, err
	}

	metaMap := make(map[string]NamespaceMetaInfo)
	replicasMap := make(map[string]map[string]PartitionReplicaInfo)
	leaderMap := make(map[string]map[string]RealLeader)
	maxEpoch := etcdReg.processNamespaceNode(resp.Kvs, metaMap, replicasMap, leaderMap)

	nsInfos := make(map[string]map[int]PartitionMetaInfo)
	if EpochType(resp.Kvs[0].ModRevision) > maxEpoch {
		maxEpoch = EpochType(resp.Kvs[0].ModRevision)
	}
	for k, v := range replicasMap {
		meta, ok := metaMap[k]
		if !ok {
			continue
		}
		partInfos, ok := nsInfos[k]
		if !ok {
			partInfos = make(map[int]PartitionMetaInfo, meta.PartitionNum)
			nsInfos[k] = partInfos
		}
		for k2, v2 := range v {
			partition, err := strconv.Atoi(k2)
			if err != nil {
				continue
			}
			if partition >= meta.PartitionNum {
				coordLog.Infof("invalid partition id : %v ", k2)
				continue
			}
			var info PartitionMetaInfo
			info.Name = k
			info.Partition = partition
			info.NamespaceMetaInfo = meta
			info.PartitionReplicaInfo = v2
			if leaders, ok := leaderMap[k]; ok {
				info.currentLeader = leaders[k2]
			}
			partInfos[partition] = info
		}
	}

	etcdReg.nsMutex.Lock()
	etcdReg.allNamespaceInfos = nsInfos
	if maxEpoch != etcdReg.nsEpoch {
		coordLog.Infof("ns epoch changed from %v to : %v ", etcdReg.nsEpoch, maxEpoch)
	}
	etcdReg.nsEpoch = maxEpoch
	etcdReg.nsMutex.Unlock()

	return nsInfos, maxEpoch, nil
}

func (etcdReg *EtcdRegister) processNamespaceNode(nodes []*mvccpb.KeyValue,
	metaMap map[string]NamespaceMetaInfo,
	replicasMap map[string]map[string]PartitionReplicaInfo,
	leaderMap map[string]map[string]RealLeader) EpochType {
	maxEpoch := EpochType(0)
	for _, node := range nodes {
		if EpochType(node.ModRevision) > maxEpoch {
			maxEpoch = EpochType(node.ModRevision)
		}
		_, key := path.Split(string(node.Key))
		if key == NAMESPACE_REPLICA_INFO {
			var rInfo PartitionReplicaInfo
			if err := json.Unmarshal([]byte(node.Value), &rInfo); err != nil {
				coordLog.Infof("unmarshal replica info %v failed: %v", node.Value, err)
				continue
			}
			rInfo.epoch = EpochType(node.ModRevision)
			keys := strings.Split(string(node.Key), "/")
			keyLen := len(keys)
			if keyLen < 3 {
				continue
			}
			nsName := keys[keyLen-3]
			partition := keys[keyLen-2]
			v, ok := replicasMap[nsName]
			if ok {
				v[partition] = rInfo
			} else {
				pMap := make(map[string]PartitionReplicaInfo)
				pMap[partition] = rInfo
				replicasMap[nsName] = pMap
			}
		} else if key == NAMESPACE_META {
			var mInfo NamespaceMetaInfo
			if err := json.Unmarshal(node.Value, &mInfo); err != nil {
				continue
			}
			keys := strings.Split(string(node.Key), "/")
			keyLen := len(keys)
			if keyLen < 2 {
				continue
			}
			mInfo.metaEpoch = EpochType(node.ModRevision)
			nsName := keys[keyLen-2]
			metaMap[nsName] = mInfo
		} else if key == NAMESPACE_REAL_LEADER {
			var rInfo RealLeader
			if err := json.Unmarshal(node.Value, &rInfo); err != nil {
				continue
			}
			rInfo.epoch = EpochType(node.ModRevision)
			keys := strings.Split(string(node.Key), "/")
			keyLen := len(keys)
			if keyLen < 3 {
				continue
			}
			nsName := keys[keyLen-3]
			partition := keys[keyLen-2]
			v, ok := leaderMap[nsName]
			if ok {
				v[partition] = rInfo
			} else {
				pMap := make(map[string]RealLeader)
				pMap[partition] = rInfo
				leaderMap[nsName] = pMap
			}
		}
	}
	return maxEpoch
}

func (etcdReg *EtcdRegister) GetNamespacePartInfo(ns string, partition int) (*PartitionMetaInfo, error) {
	etcdReg.nsMutex.Lock()
	defer etcdReg.nsMutex.Unlock()
	nsInfo, ok := etcdReg.allNamespaceInfos[ns]
	if !ok {
		return nil, ErrKeyNotFound
	}
	p, ok := nsInfo[partition]
	if !ok {
		return nil, ErrKeyNotFound
	}
	if p.Partition != partition {
		panic(p)
	}
	return p.GetCopy(), nil
}

//[finish]
func (etcdReg *EtcdRegister) GetRemoteNamespaceReplicaInfo(ns string, partition int) (*PartitionReplicaInfo, error) {
	resp, err := kvGet(etcdReg.client, etcdReg.getNamespaceReplicaInfoPath(ns, partition))
	if err != nil {
		return nil, err
	}
	var rInfo PartitionReplicaInfo
	if err = json.Unmarshal(resp.Kvs[0].Value, &rInfo); err != nil {
		return nil, err
	}
	rInfo.epoch = EpochType(resp.Kvs[0].ModRevision)
	return &rInfo, nil
}

//[finish]
func (etcdReg *EtcdRegister) GetNamespaceTableSchema(ns string, table string) (*SchemaInfo, error) {
	resp, err := kvGet(etcdReg.client, etcdReg.getNamespaceTableSchemaPath(ns, table))
	if err != nil {
		return nil, err
	}

	var info SchemaInfo
	info.Schema = resp.Kvs[0].Value
	info.Epoch = EpochType(resp.Kvs[0].ModRevision)
	return &info, nil
}

func (etcdReg *EtcdRegister) GetNamespaceInfo(ns string) ([]PartitionMetaInfo, error) {
	etcdReg.nsMutex.Lock()
	defer etcdReg.nsMutex.Unlock()
	nsInfo, ok := etcdReg.allNamespaceInfos[ns]
	if !ok {
		return nil, ErrKeyNotFound
	}
	parts := make([]PartitionMetaInfo, 0, len(nsInfo))
	for _, v := range nsInfo {
		parts = append(parts, *v.GetCopy())
	}
	return parts, nil
}

//[finish]
func (etcdReg *EtcdRegister) GetNamespaceMetaInfo(ns string) (NamespaceMetaInfo, error) {
	etcdReg.nsMutex.Lock()
	parts, ok := etcdReg.allNamespaceInfos[ns]
	etcdReg.nsMutex.Unlock()
	var meta NamespaceMetaInfo
	if !ok || len(parts) == 0 {
		resp, err := kvGet(etcdReg.client, etcdReg.getNamespaceMetaPath(ns))
		if err != nil {
			return meta, err
		}
		err = json.Unmarshal(resp.Kvs[0].Value, &meta)
		if err != nil {
			return meta, err
		}
		meta.metaEpoch = EpochType(resp.Kvs[0].ModRevision)
		return meta, nil
	}
	return parts[0].NamespaceMetaInfo, nil
}

func (etcdReg *EtcdRegister) getClusterPath() string {
	return path.Join("/", ROOT_DIR, etcdReg.clusterID)
}

func (etcdReg *EtcdRegister) getClusterMetaPath() string {
	return path.Join(etcdReg.getClusterPath(), CLUSTER_META_INFO)
}

func (etcdReg *EtcdRegister) getPDNodePath(value *NodeInfo) string {
	return path.Join(etcdReg.getPDNodeRootPath(), "Node-"+value.ID)
}

func (etcdReg *EtcdRegister) getPDNodeRootPath() string {
	return path.Join(etcdReg.getClusterPath(), PD_ROOT_DIR, PD_NODE_DIR)
}

func (etcdReg *EtcdRegister) getPDLeaderPath() string {
	return path.Join(etcdReg.getClusterPath(), PD_ROOT_DIR, PD_LEADER_SESSION)
}

func (etcdReg *EtcdRegister) getDataNodeRootPath() string {
	return path.Join(etcdReg.getClusterPath(), DATA_NODE_DIR)
}

func (etcdReg *EtcdRegister) getNamespaceRootPath() string {
	return path.Join(etcdReg.getClusterPath(), NAMESPACE_DIR)
}

func (etcdReg *EtcdRegister) getNamespacePath(ns string) string {
	return path.Join(etcdReg.namespaceRoot, ns)
}

func (etcdReg *EtcdRegister) getNamespaceMetaPath(ns string) string {
	return path.Join(etcdReg.getNamespacePath(ns), NAMESPACE_META)
}

func (etcdReg *EtcdRegister) getNamespaceSchemaPath(ns string) string {
	return path.Join(etcdReg.getNamespacePath(ns), NAMESPACE_SCHEMA)
}

func (etcdReg *EtcdRegister) getNamespaceTableSchemaPath(ns string, table string) string {
	return path.Join(etcdReg.getNamespaceSchemaPath(ns), table)
}

func (etcdReg *EtcdRegister) getNamespacePartitionPath(ns string, partition int) string {
	return path.Join(etcdReg.getNamespacePath(ns), strconv.Itoa(partition))
}

func (etcdReg *EtcdRegister) getNamespaceReplicaInfoPath(ns string, partition int) string {
	return path.Join(etcdReg.getNamespacePartitionPath(ns, partition), NAMESPACE_REPLICA_INFO)
}

// placement driver register
type PDEtcdRegister struct {
	*EtcdRegister

	leaderSessionPath string
	leaderStr         string
	nodeInfo          *NodeInfo
	nodeKey           string
	nodeValue         string

	refreshStopCh chan bool
}

func NewPDEtcdRegister(host string) *PDEtcdRegister {
	return &PDEtcdRegister{
		EtcdRegister:  NewEtcdRegister(host),
		refreshStopCh: make(chan bool, 1),
	}
}

func (etcdReg *PDEtcdRegister) Register(value *NodeInfo) error {
	etcdReg.leaderSessionPath = etcdReg.getPDLeaderPath()
	etcdReg.nodeInfo = value
	valueB, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if etcdReg.refreshStopCh != nil {
		close(etcdReg.refreshStopCh)
	}

	etcdReg.leaderStr = string(valueB)
	etcdReg.nodeKey = etcdReg.getPDNodePath(value)
	etcdReg.nodeValue = string(valueB)
	etcdReg.refreshStopCh = make(chan bool)
	go register(etcdReg.client, etcdReg.nodeKey, etcdReg.nodeValue, ETCD_TTL, etcdReg.refreshStopCh)

	return nil
}

func (etcdReg *PDEtcdRegister) Unregister(value *NodeInfo) error {
	// stop to refresh
	if etcdReg.refreshStopCh != nil {
		close(etcdReg.refreshStopCh)
		etcdReg.refreshStopCh = nil
	}

	err := kvDel(etcdReg.client, etcdReg.getPDNodePath(value))
	if err != nil {
		coordLog.Warningf("cluser[%s] node[%s] unregister failed: %v", etcdReg.clusterID, value, err)
		return err
	}

	return nil
}

//[finish]
func (etcdReg *PDEtcdRegister) PrepareNamespaceMinGID() (int64, error) {
	var clusterMeta ClusterMetaInfo
	initValue, _ := json.Marshal(clusterMeta)
	err := exchangeNodeValue(
		etcdReg.client,
		etcdReg.getClusterMetaPath(),
		string(initValue),
		func(isNew bool, oldValue string) (string, error) {
			if !isNew && oldValue != "" {
				err := json.Unmarshal([]byte(oldValue), &clusterMeta)
				if err != nil {
					coordLog.Infof("cluster meta: %v", string(oldValue))
					return "", err
				}
			}
			clusterMeta.MaxGID += 10000
			newValue, err := json.Marshal(clusterMeta)
			coordLog.Infof("updating cluster meta: %v", clusterMeta)
			return string(newValue), err
		})

	return clusterMeta.MaxGID, err
}

func (etcdReg *PDEtcdRegister) GetClusterMetaInfo() (ClusterMetaInfo, error) {
	var clusterMeta ClusterMetaInfo
	resp, err := kvGet(etcdReg.client, etcdReg.getClusterMetaPath())
	if err != nil {
		return clusterMeta, err
	}
	err = json.Unmarshal(resp.Kvs[0].Value, &clusterMeta)
	return clusterMeta, err
}

func (etcdReg *PDEtcdRegister) GetClusterEpoch() (EpochType, error) {
	resp, err := kvGet(etcdReg.client, etcdReg.clusterPath)
	if err != nil {
		return 0, err
	}
	return EpochType(resp.Kvs[0].ModRevision), nil
}

func (etcdReg *PDEtcdRegister) AcquireAndWatchLeader(leader chan *NodeInfo, stop chan struct{}) {

	for {
		select {
		case <-stop:
			close(leader)
			return
		default:

		}
		t := etcdReg.client.Txn(context.Background()).If(clientv3.Compare(clientv3.CreateRevision(etcdReg.leaderSessionPath), "=", 0))
		t = t.Then(clientv3.OpPut(etcdReg.leaderSessionPath, etcdReg.leaderStr))
		t = t.Else(clientv3.OpGet(etcdReg.leaderSessionPath))
		resp, err := t.Commit()
		if err != nil {
			coordLog.Errorf("Acquire Leader key[%s] error:%s", etcdReg.leaderSessionPath, err.Error())
			time.Sleep(time.Millisecond * 10)
			continue
		}
		if resp.Succeeded {
			var node NodeInfo
			if err := json.Unmarshal([]byte(etcdReg.leaderStr), &node); err != nil {
				leader <- &node
				continue
			}
			coordLog.Infof("Leader Node[%v]", node)
			leader <- &node
		} else {
			kv := resp.Responses[0].GetResponseRange().Kvs[0]
			var node NodeInfo
			if err := json.Unmarshal(kv.Value, &node); err != nil {
				leader <- &node
				continue
			}
			coordLog.Infof("Leader Node[%v]", node)
			leader <- &node
		}
		break
	}

	watcher := createWatch(etcdReg.client, etcdReg.leaderSessionPath)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			cancel()
		}
	}()

	for {
		_, err := watcher.WaitNext(ctx, 0)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", etcdReg.leaderSessionPath)
				close(leader)
				return
			}
		}

		resp, err := kvGet(etcdReg.client, etcdReg.leaderSessionPath)
		if err == nil {
			coordLog.Infof("key: %s value: %s", string(resp.Kvs[0].Key), string(resp.Kvs[0].Value))
			var node NodeInfo
			err = json.Unmarshal(resp.Kvs[0].Value, &node)
			if err == nil {
				select {
				case leader <- &node:
				case <-stop:
					close(leader)
					return
				}
			}
		}
	}
}

//[finish]
func (etcdReg *PDEtcdRegister) CheckIfLeader() bool {
	resp, err := kvGet(etcdReg.client, etcdReg.leaderSessionPath)
	if err != nil {
		return false
	}
	if string(resp.Kvs[0].Value) == etcdReg.leaderStr {
		return true
	}
	return false
}

func (etcdReg *PDEtcdRegister) GetDataNodes() ([]NodeInfo, error) {
	return etcdReg.getDataNodes()
}

//[finish]
func (etcdReg *PDEtcdRegister) WatchDataNodes(dataNodesChan chan []NodeInfo, stop chan struct{}) {
	dataNodes, err := etcdReg.getDataNodes()
	if err == nil {
		select {
		case dataNodesChan <- dataNodes:
		case <-stop:
			close(dataNodesChan)
			return
		}
	}

	key := etcdReg.getDataNodeRootPath()
	watcher := createWatch(etcdReg.client, key)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			cancel()
		}
	}()
	for {
		_, err := watcher.WaitNext(ctx, 0)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", key)
				close(dataNodesChan)
				return
			}
		}
		dataNodes, err := etcdReg.getDataNodes()
		if err != nil {
			coordLog.Errorf("key[%s] getNodes error: %s", key, err.Error())
			continue
		}
		select {
		case dataNodesChan <- dataNodes:
		case <-stop:
			close(dataNodesChan)
			return
		}
	}
}

//[finish]
func (etcdReg *PDEtcdRegister) getDataNodes() ([]NodeInfo, error) {
	resp, err := kvGet(etcdReg.client, etcdReg.getDataNodeRootPath())
	if err != nil {
		return nil, err
	}
	dataNodes := make([]NodeInfo, 0)
	for _, node := range resp.Kvs {
		var nodeInfo NodeInfo
		err := json.Unmarshal(node.Value, &nodeInfo)
		if err != nil {
			continue
		}
		dataNodes = append(dataNodes, nodeInfo)
	}
	return dataNodes, nil
}

//[finish]
func (etcdReg *PDEtcdRegister) CreateNamespacePartition(ns string, partition int) error {
	_, err := kvSave(etcdReg.client, etcdReg.getNamespacePartitionPath(ns, partition), "")
	if err != nil {
		return err
	}
	return nil
}

//[finish]
func (etcdReg *PDEtcdRegister) CreateNamespace(ns string, meta *NamespaceMetaInfo) error {
	if meta.MinGID <= 0 {
		return errors.New("namespace MinGID is invalid")
	}
	metaValue, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	resp, err := kvSave(etcdReg.client, etcdReg.getNamespaceMetaPath(ns), string(metaValue))
	if err != nil {
		return err
	}

	meta.metaEpoch = EpochType(resp.PrevKv.ModRevision)
	return nil
}

//[finish]
func (etcdReg *PDEtcdRegister) IsExistNamespace(ns string) (bool, error) {
	_, err := kvGet(etcdReg.client, etcdReg.getNamespacePath(ns))
	if err != nil {
		return false, err
	}
	return true, nil
}

//[finish]
func (etcdReg *PDEtcdRegister) IsExistNamespacePartition(ns string, partitionNum int) (bool, error) {
	_, err := kvGet(etcdReg.client, etcdReg.getNamespacePartitionPath(ns, partitionNum))
	if err != nil {
		return false, err
	}
	return true, nil
}

//[finish]
func (etcdReg *PDEtcdRegister) UpdateNamespaceMetaInfo(ns string, meta *NamespaceMetaInfo, oldGen EpochType) error {
	value, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	coordLog.Infof("Update meta info: %s %s %d", ns, string(value), oldGen)

	etcdReg.nsMutex.Lock()
	defer etcdReg.nsMutex.Unlock()
	atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)

	resp, err := kvCompareAndSwap(etcdReg.client, etcdReg.getNamespaceMetaPath(ns), string(value), int64(oldGen))
	if err != nil {
		return err
	}
	err = json.Unmarshal(resp.Responses[0].GetResponseRange().Kvs[0].Value, &meta)
	if err != nil {
		coordLog.Errorf("unmarshal meta info failed: %v, %v", err, string(resp.Responses[0].GetResponseRange().Kvs[0].Value))
		return err
	}
	meta.metaEpoch = EpochType(resp.Responses[0].GetResponseRange().Kvs[0].ModRevision)

	return nil
}

//[finish]
func (etcdReg *PDEtcdRegister) DeleteWholeNamespace(ns string) error {
	etcdReg.nsMutex.Lock()
	atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
	err := kvDel(etcdReg.client, etcdReg.getNamespacePath(ns))
	coordLog.Infof("delete whole topic: %v, %v", ns, err)
	etcdReg.nsMutex.Unlock()
	return err
}

//[finish]
func (etcdReg *PDEtcdRegister) DeleteNamespacePart(ns string, partition int) error {
	err := kvDel(etcdReg.client, etcdReg.getNamespacePartitionPath(ns, partition))
	if err != nil {
		return err
	}
	return nil
}

//[finish]
func (etcdReg *PDEtcdRegister) UpdateNamespacePartReplicaInfo(ns string, partition int,
	replicaInfo *PartitionReplicaInfo, oldGen EpochType) error {
	value, err := json.Marshal(replicaInfo)
	if err != nil {
		return err
	}
	coordLog.Infof("Update info: %s %d %s %d", ns, partition, string(value), oldGen)
	if oldGen == 0 {
		resp, err := kvCompareAndSwap(etcdReg.client, etcdReg.getNamespaceReplicaInfoPath(ns, partition), string(value), int64(oldGen))
		if err != nil {
			return err
		}
		replicaInfo.epoch = EpochType(resp.Responses[0].GetResponseRange().Kvs[0].ModRevision)
		return nil
	}
	resp, err := kvCompareAndSwap(etcdReg.client, etcdReg.getNamespaceReplicaInfoPath(ns, partition), string(value), int64(oldGen))
	if err != nil {
		return err
	}
	replicaInfo.epoch = EpochType(resp.Responses[0].GetResponseRange().Kvs[0].ModRevision)
	return nil
}

//[finish]
func (etcdReg *PDEtcdRegister) UpdateNamespaceSchema(ns string, table string, schema *SchemaInfo) error {
	if schema.Epoch == 0 {
		resp, err := kvCompareAndSwap(etcdReg.client, etcdReg.getNamespaceTableSchemaPath(ns, table), string(schema.Schema), 0)
		if err != nil {
			return err
		}
		schema.Epoch = EpochType(resp.Responses[0].GetResponseRange().Kvs[0].ModRevision)
		return nil
	}

	resp, err := kvCompareAndSwap(etcdReg.client, etcdReg.getNamespaceTableSchemaPath(ns, table), string(schema.Schema), int64(schema.Epoch))
	if err != nil {
		return err
	}
	schema.Epoch = EpochType(resp.Responses[0].GetResponseRange().Kvs[0].ModRevision)
	return nil
}

type DNEtcdRegister struct {
	*EtcdRegister
	sync.Mutex

	nodeKey       string
	nodeValue     string
	refreshStopCh chan bool
}

func NewDNEtcdRegister(host string) *DNEtcdRegister {
	return &DNEtcdRegister{
		EtcdRegister: NewEtcdRegister(host),
	}
}

func (etcdReg *DNEtcdRegister) Register(nodeData *NodeInfo) error {
	if nodeData.LearnerRole != "" &&
		nodeData.LearnerRole != common.LearnerRoleLogSyncer &&
		nodeData.LearnerRole != common.LearnerRoleSearcher {
		return ErrLearnerRoleUnsupported
	}
	value, err := json.Marshal(nodeData)
	if err != nil {
		return err
	}
	etcdReg.nodeKey = etcdReg.getDataNodePath(nodeData)
	resp, err := kvGet(etcdReg.client, etcdReg.nodeKey)
	if err != nil {
		return err
	} else {
		var node NodeInfo
		err = json.Unmarshal(resp.Kvs[0].Value, &node)
		if err != nil {
			return err
		}
		if node.LearnerRole != nodeData.LearnerRole {
			coordLog.Warningf("node learner role should never be changed: %v (old %v)", nodeData.LearnerRole, node.LearnerRole)
			return ErrLearnerRoleInvalidChanged
		}
	}
	if etcdReg.refreshStopCh != nil {
		close(etcdReg.refreshStopCh)
	}

	etcdReg.nodeValue = string(value)
	coordLog.Infof("registered new node: %v", nodeData)
	etcdReg.refreshStopCh = make(chan bool)
	go register(etcdReg.client, etcdReg.nodeKey, etcdReg.nodeValue, ETCD_TTL, etcdReg.refreshStopCh)

	return nil
}

//[finish]
func (etcdReg *DNEtcdRegister) Unregister(nodeData *NodeInfo) error {
	etcdReg.Lock()
	defer etcdReg.Unlock()

	// stop refresh
	if etcdReg.refreshStopCh != nil {
		close(etcdReg.refreshStopCh)
		etcdReg.refreshStopCh = nil
	}

	err := kvDel(etcdReg.client, etcdReg.getDataNodePath(nodeData))
	if err != nil {
		coordLog.Warningf("cluser[%s] node[%s] unregister failed: %v", etcdReg.clusterID, nodeData, err)
		return err
	}

	coordLog.Infof("cluser[%s] node[%v] unregistered", etcdReg.clusterID, nodeData)
	return nil
}

func (etcdReg *DNEtcdRegister) GetNamespaceLeader(ns string, partition int) (string, EpochType, error) {
	etcdReg.nsMutex.Lock()
	defer etcdReg.nsMutex.Unlock()
	nsInfo, ok := etcdReg.allNamespaceInfos[ns]
	if !ok {
		return "", 0, ErrKeyNotFound
	}
	p, ok := nsInfo[partition]
	if !ok {
		return "", 0, ErrKeyNotFound
	}
	return p.GetRealLeader(), p.currentLeader.epoch, nil
}

//[finish]
func (etcdReg *DNEtcdRegister) UpdateNamespaceLeader(ns string, partition int, rl RealLeader, oldGen EpochType) (EpochType, error) {
	value, err := json.Marshal(rl)
	if err != nil {
		return oldGen, err
	}
	if oldGen == 0 {
		resp, err := kvCompareAndSwap(etcdReg.client, etcdReg.getNamespaceLeaderPath(ns, partition), string(value), 0)
		if err != nil {
			return 0, err
		}
		rl.epoch = EpochType(resp.Responses[0].GetResponseRange().Kvs[0].ModRevision)
		return rl.epoch, nil
	}
	resp, err := kvCompareAndSwap(etcdReg.client, etcdReg.getNamespaceLeaderPath(ns, partition), string(value), int64(oldGen))
	if err != nil {
		return 0, err
	}
	rl.epoch = EpochType(resp.Responses[0].GetResponseRange().Kvs[0].ModRevision)
	return rl.epoch, nil
}

//[finish]
func (etcdReg *DNEtcdRegister) GetNodeInfo(nid string) (NodeInfo, error) {
	var node NodeInfo
	resp, err := kvGet(etcdReg.client, etcdReg.getDataNodePathFromID(nid))
	if err != nil {
		return node, err
	}
	err = json.Unmarshal(resp.Kvs[0].Value, &node)
	if err != nil {
		return node, err
	}
	return node, nil
}

func (etcdReg *DNEtcdRegister) NewRegisterNodeID() (uint64, error) {
	var clusterMeta ClusterMetaInfo
	initValue, _ := json.Marshal(clusterMeta)
	exchangeErr := exchangeNodeValue(etcdReg.client, etcdReg.getClusterMetaPath(), string(initValue), func(isNew bool, oldValue string) (string, error) {
		if !isNew && oldValue != "" {
			err := json.Unmarshal([]byte(oldValue), &clusterMeta)
			if err != nil {
				return "", err
			}
		}
		clusterMeta.MaxRegID += 1
		newValue, err := json.Marshal(clusterMeta)
		return string(newValue), err
	})
	return clusterMeta.MaxRegID, exchangeErr
}

//[finish]
func (etcdReg *DNEtcdRegister) WatchPDLeader(leader chan *NodeInfo, stop chan struct{}) error {
	key := etcdReg.getPDLeaderPath()
	resp, err := kvGet(etcdReg.client, key)
	if err == nil {
		coordLog.Infof("key: %s value: %s", string(resp.Kvs[0].Key), string(resp.Kvs[0].Value))
		var node NodeInfo
		err = json.Unmarshal(resp.Kvs[0].Value, &node)
		if err == nil {
			select {
			case leader <- &node:
			case <-stop:
				close(leader)
				return nil
			}
		}
	} else {
		coordLog.Errorf("get error: %s", err.Error())
	}

	watcher := createWatch(etcdReg.client, key)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			cancel()
		}
	}()

	for {
		_, err := watcher.WaitNext(ctx, 0)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", key)
				close(leader)
				return err
			}
		}

		resp, err := kvGet(etcdReg.client, key)
		if err == nil {
			coordLog.Infof("key: %s value: %s", string(resp.Kvs[0].Key), string(resp.Kvs[0].Value))
			var node NodeInfo
			err = json.Unmarshal(resp.Kvs[0].Value, &node)
			if err == nil {
				select {
				case leader <- &node:
				case <-stop:
					close(leader)
					return nil
				}
			}
		}
	}
}

func (etcdReg *DNEtcdRegister) getDataNodePathFromID(nid string) string {
	return path.Join(etcdReg.getClusterPath(), DATA_NODE_DIR, "Node-"+nid)
}

func (etcdReg *DNEtcdRegister) getDataNodePath(nodeData *NodeInfo) string {
	return path.Join(etcdReg.getClusterPath(), DATA_NODE_DIR, "Node-"+nodeData.ID)
}

func (etcdReg *DNEtcdRegister) getNamespaceLeaderPath(ns string, partition int) string {
	return path.Join(etcdReg.getNamespacePartitionPath(ns, partition), NAMESPACE_REAL_LEADER)
}
