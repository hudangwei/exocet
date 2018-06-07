package pdnode_coord

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/divebomb/exocet/cluster"
	"github.com/divebomb/exocet/common"
	"github.com/spaolacci/murmur3"
)

var (
	ErrBalanceNodeUnavailable = errors.New("can not find a node to be balanced")
	ErrClusterBalanceRunning  = errors.New("another balance is running, should wait")
)

type balanceOpLevel int

func splitNamespacePartitionID(namespaceFullName string) (string, int, error) {
	partIndex := strings.LastIndex(namespaceFullName, "-")
	if partIndex == -1 {
		return "", 0, fmt.Errorf("invalid namespace full name: %v", namespaceFullName)
	}
	namespaceName := namespaceFullName[:partIndex]
	partitionID, err := strconv.Atoi(namespaceFullName[partIndex+1:])
	return namespaceName, partitionID, err
}

// An IntHeap is a min-heap of ints.
type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// @currentNodes: {key: node id, value: node info}
func getNodeNameList(currentNodes map[string]cluster.NodeInfo) []SortableStrings {
	nodeNameMap := make(map[string]SortableStrings) // key: data center name, value: node id list
	dcInfoList := make(SortableStrings, 0)
	// get nodeNameMap by @currentNodes
	for nid, ninfo := range currentNodes {
		dcInfo := ""
		dc, ok := ninfo.Tags[cluster.DCInfoTag]
		if ok {
			dcInfo, _ = dc.(string)
		}
		nodeNameMap[dcInfo] = append(nodeNameMap[dcInfo], nid)
	}
	// get dcInfoList by nodeNameMap
	// cause the dcInfo is the key of nodeNameMap, there is no same dcInfo in dcInfoList
	for dcInfo := range nodeNameMap {
		dcInfoList = append(dcInfoList, dcInfo)
	}
	sort.Sort(dcInfoList)
	nodeNameList := make([]SortableStrings, 0, len(nodeNameMap))
	// get nodeNameList by @dcInfoList
	for _, dc := range dcInfoList {
		sort.Sort(nodeNameMap[dc]) // 此处可见 nodeNameList[idx] 都是一个 idc 中的 node集合，按照 node name 的字母顺序进行了排序
		nodeNameList = append(nodeNameList, nodeNameMap[dc])
	}
	return nodeNameList // nodeNameList 第一维按照 idc name 有序，第二维则是按照 node name 的字典顺序有序
}

// query the raft peers if the nid already in the raft group for the namespace in all raft peers
func IsRaftNodeJoined(nsInfo *cluster.PartitionMetaInfo, nid string) (bool, error) {
	if len(nsInfo.RaftNodes) == 0 {
		return false, nil
	}
	var lastErr error
	for _, remoteNode := range nsInfo.GetISR() {
		if remoteNode == nid {
			continue
		}
		nip, _, _, httpPort := cluster.ExtractNodeInfoFromID(remoteNode)
		var rsp []*common.MemberInfo
		_, err := common.APIRequest("GET",
			"http://"+net.JoinHostPort(nip, httpPort)+common.APIGetMembers+"/"+nsInfo.GetDesp(),
			nil, time.Second*3, &rsp)
		if err != nil {
			cluster.CoordLog().Infof("failed (%v) to get members for namespace %v: %v", nip, nsInfo.GetDesp(), err)
			lastErr = err
			continue
		}

		for _, m := range rsp {
			if m.NodeID == cluster.ExtractRegIDFromGenID(nid) && m.ID == nsInfo.RaftIDs[nid] {
				cluster.CoordLog().Infof("namespace %v node %v is still in raft from %v ", nsInfo.GetDesp(), nid, remoteNode)
				return true, nil
			}
		}
	}
	return false, lastErr
}

// query the raft peers if the nid already in the raft group for the namespace and all logs synced in all peers
func IsAllISRFullReady(nsInfo *cluster.PartitionMetaInfo) (bool, error) {
	for _, nid := range nsInfo.GetISR() {
		ok, err := IsRaftNodeFullReady(nsInfo, nid)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// query the raft peers if the nid already in the raft group for the namespace and all logs synced in all peers
func IsRaftNodeFullReady(nsInfo *cluster.PartitionMetaInfo, nid string) (bool, error) {
	if len(nsInfo.RaftNodes) == 0 {
		return false, nil
	}
	for _, remoteNode := range nsInfo.GetISR() {
		nip, _, _, httpPort := cluster.ExtractNodeInfoFromID(remoteNode)
		var rsp []*common.MemberInfo
		_, err := common.APIRequest("GET",
			"http://"+net.JoinHostPort(nip, httpPort)+common.APIGetMembers+"/"+nsInfo.GetDesp(),
			nil, time.Second*3, &rsp)
		if err != nil {
			cluster.CoordLog().Infof("failed (%v) to get members for namespace %v: %v", nip, nsInfo.GetDesp(), err)
			return false, err
		}

		found := false
		for _, m := range rsp {
			if m.NodeID == cluster.ExtractRegIDFromGenID(nid) && m.ID == nsInfo.RaftIDs[nid] {
				found = true
				break
			}
		}
		if !found {
			cluster.CoordLog().Infof("raft %v not found in the node (%v) members for namespace %v", nid, nip, nsInfo.GetDesp())
			return false, nil
		}
		_, err = common.APIRequest("GET",
			"http://"+net.JoinHostPort(nip, httpPort)+common.APIIsRaftSynced+"/"+nsInfo.GetDesp(),
			nil, time.Second*5, nil)
		if err != nil {
			cluster.CoordLog().Infof("failed (%v) to check sync state for namespace %v: %v", nip, nsInfo.GetDesp(), err)
			return false, err
		}
	}
	return true, nil
}

type DataPlacement struct {
	balanceInterval [2]int32
	pdCoord         *PDCoordinator
}

func NewDataPlacement(coord *PDCoordinator) *DataPlacement {
	return &DataPlacement{
		pdCoord:         coord,
		balanceInterval: [2]int32{2, 4},
	}
}

func (dp *DataPlacement) SetBalanceInterval(start int, end int) {
	if start == end && start == 0 {
		return
	}
	atomic.StoreInt32(&dp.balanceInterval[0], int32(start))
	atomic.StoreInt32(&dp.balanceInterval[1], int32(end))
}

func (dp *DataPlacement) DoBalance(monitorChan chan struct{}) {
	//check period for the data balance.
	ticker := time.NewTicker(balanceCheckInterval)
	defer func() {
		ticker.Stop()
		cluster.CoordLog().Infof("balance check exit.")
	}()
	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			// only balance at given interval
			if time.Now().Hour() > int(atomic.LoadInt32(&dp.balanceInterval[1])) ||
				time.Now().Hour() < int(atomic.LoadInt32(&dp.balanceInterval[0])) {
				continue
			}
			if !dp.pdCoord.IsMineLeader() {
				cluster.CoordLog().Infof("not leader while checking balance")
				continue
			}
			if !dp.pdCoord.IsClusterStable() {
				cluster.CoordLog().Infof("no balance since cluster is not stable while checking balance")
				continue
			}
			if !dp.pdCoord.AutoBalanceEnabled() {
				continue
			}
			cluster.CoordLog().Infof("begin checking balance of namespace data...")
			currentNodes := dp.pdCoord.getCurrentNodes(nil)
			validNum := len(currentNodes)
			if validNum < 2 {
				continue
			}
			dp.rebalanceNamespace(monitorChan)
		}
	}
}

func (dp *DataPlacement) addNodeToNamespaceAndWaitReady(monitorChan chan struct{}, namespaceInfo *cluster.PartitionMetaInfo,
	nodeNameList []SortableStrings) (*cluster.PartitionMetaInfo, error) {
	retry := 0
	currentSelect := 0
	namespaceName := namespaceInfo.Name
	partitionID := namespaceInfo.Partition
	// since we need add new catchup, we make the replica as replica+1
	partitionNodes, coordErr := getRebalancedPartitionsFromNameList(
		namespaceInfo.Name,
		namespaceInfo.PartitionNum,
		namespaceInfo.Replica+1, nodeNameList)
	if coordErr != nil {
		return namespaceInfo, coordErr.ToErrorType()
	}
	fullName := namespaceInfo.GetDesp()
	selectedCatchup := make([]string, 0)
	for _, nid := range partitionNodes[namespaceInfo.Partition] {
		if cluster.FindSlice(namespaceInfo.RaftNodes, nid) != -1 {
			// already isr, ignore add catchup
			continue
		}
		selectedCatchup = append(selectedCatchup, nid)
	}
	var nInfo *cluster.PartitionMetaInfo
	var err error
	for {
		if currentSelect >= len(selectedCatchup) {
			cluster.CoordLog().Infof("currently no any node %v can be balanced for namespace: %v, expect isr: %v, nodes:%v",
				selectedCatchup, fullName, partitionNodes[partitionID], nodeNameList)
			return nInfo, ErrBalanceNodeUnavailable
		}
		nid := selectedCatchup[currentSelect]
		nInfo, err = dp.pdCoord.register.GetNamespacePartInfo(namespaceName, partitionID)
		if err != nil {
			cluster.CoordLog().Infof("failed to get namespace %v info: %v", fullName, err)
		} else {
			if inRaft, _ := IsRaftNodeFullReady(nInfo, nid); inRaft {
				break
			} else if cluster.FindSlice(nInfo.RaftNodes, nid) != -1 {
				// wait ready
				select {
				case <-monitorChan:
					return nInfo, errors.New("quiting")
				case <-time.After(time.Second * 5):
					cluster.CoordLog().Infof("node: %v is added for namespace %v , still waiting catchup", nid, nInfo.GetDesp())
				}
				continue
			} else {
				if ok, err := IsAllISRFullReady(nInfo); err != nil || !ok {
					cluster.CoordLog().Infof("namespace %v isr %v are not full ready while adding node", nInfo.GetDesp(), nInfo.RaftNodes)
					return nInfo, fmt.Errorf("namespace %v isr are not full ready", nInfo.GetDesp())
				}
				cluster.CoordLog().Infof("node: %v is added for namespace %v: (%v)", nid, nInfo.GetDesp(), nInfo.RaftNodes)
				coordErr = dp.pdCoord.addNamespaceToNode(nInfo, nid)
				if coordErr != nil {
					cluster.CoordLog().Infof("node: %v added for namespace %v (%v) failed: %v", nid,
						nInfo.GetDesp(), nInfo.RaftNodes, coordErr)
					currentSelect++
				}
			}
		}
		select {
		case <-monitorChan:
			return nInfo, errors.New("quiting")
		case <-time.After(time.Second * 5):
		}
		if retry > 5 {
			cluster.CoordLog().Infof("add catchup and wait timeout : %v", fullName)
			return nInfo, errors.New("wait timeout")
		}
		retry++
	}
	return nInfo, nil
}

func (dp *DataPlacement) getExcludeNodesForNamespace(namespaceInfo *cluster.PartitionMetaInfo) (map[string]struct{}, error) {
	excludeNodes := make(map[string]struct{})
	for _, v := range namespaceInfo.RaftNodes {
		excludeNodes[v] = struct{}{}
	}
	return excludeNodes, nil
}

func (dp *DataPlacement) allocNodeForNamespace(namespaceInfo *cluster.PartitionMetaInfo,
	currentNodes map[string]cluster.NodeInfo) (*cluster.NodeInfo, *cluster.CoordErr) {
	var chosenNode cluster.NodeInfo
	excludeNodes, commonErr := dp.getExcludeNodesForNamespace(namespaceInfo)
	if commonErr != nil {
		return nil, cluster.ErrRegisterServiceUnstable
	}

	partitionNodes, err := getRebalancedNamespacePartitions(
		namespaceInfo.Name,
		namespaceInfo.PartitionNum,
		namespaceInfo.Replica, currentNodes)
	if err != nil {
		return nil, err
	}
	for _, nodeID := range partitionNodes[namespaceInfo.Partition] {
		if _, ok := excludeNodes[nodeID]; ok {
			continue
		}
		chosenNode = currentNodes[nodeID]
		break
	}
	if chosenNode.ID == "" {
		cluster.CoordLog().Infof("no more available node for namespace: %v, excluding nodes: %v, all nodes: %v",
			namespaceInfo.GetDesp(), excludeNodes, currentNodes)
		return nil, ErrNodeUnavailable
	}
	cluster.CoordLog().Infof("node %v is alloc for namespace: %v", chosenNode, namespaceInfo.GetDesp())
	return &chosenNode, nil
}

func (dp *DataPlacement) checkNamespaceNodeConflict(namespaceInfo *cluster.PartitionMetaInfo) bool {
	existSlaves := make(map[string]struct{})
	// isr should be different
	for _, id := range namespaceInfo.RaftNodes {
		if _, ok := existSlaves[id]; ok {
			return false
		}
		existSlaves[id] = struct{}{}
	}
	return true
}

func (dp *DataPlacement) allocNamespaceRaftNodes(ns string, currentNodes map[string]cluster.NodeInfo,
	replica int, partitionNum int, existPart map[int]*cluster.PartitionMetaInfo) ([]cluster.PartitionReplicaInfo, *cluster.CoordErr) {
	replicaList := make([]cluster.PartitionReplicaInfo, partitionNum)
	partitionNodes, err := getRebalancedNamespacePartitions(
		ns,
		partitionNum,
		replica, currentNodes)
	if err != nil {
		return nil, err
	}
	for p := 0; p < partitionNum; p++ {
		var replicaInfo cluster.PartitionReplicaInfo
		if elem, ok := existPart[p]; ok {
			replicaInfo = elem.PartitionReplicaInfo
		} else {
			replicaInfo.RaftNodes = partitionNodes[p]
			replicaInfo.RaftIDs = make(map[string]uint64) // 下面 for 循环计算出来的结果肯定是 {1, 2, 3}
			replicaInfo.Removings = make(map[string]cluster.RemovingInfo)
			for _, nid := range replicaInfo.RaftNodes {
				replicaInfo.MaxRaftID++
				replicaInfo.RaftIDs[nid] = uint64(replicaInfo.MaxRaftID)
			}
		}
		replicaList[p] = replicaInfo
	}

	cluster.CoordLog().Infof("selected namespace replica list : %v", replicaList)
	return replicaList, nil
}

func (dp *DataPlacement) rebalanceNamespace(monitorChan chan struct{}) (bool, bool) {
	moved := false
	isAllBalanced := false
	if !atomic.CompareAndSwapInt32(&dp.pdCoord.balanceWaiting, 0, 1) {
		cluster.CoordLog().Infof("another balance is running, should wait")
		return moved, isAllBalanced
	}
	defer atomic.StoreInt32(&dp.pdCoord.balanceWaiting, 0)

	allNamespaces, _, err := dp.pdCoord.register.GetAllNamespaces()
	if err != nil {
		cluster.CoordLog().Infof("scan namespaces error: %v", err)
		return moved, isAllBalanced
	}
	namespaceList := make([]cluster.PartitionMetaInfo, 0)
	for _, parts := range allNamespaces {
		for _, p := range parts {
			namespaceList = append(namespaceList, *(p.GetCopy()))
		}
	}
	movedNamespace := ""
	isAllBalanced = true
	for _, namespaceInfo := range namespaceList {
		select {
		case <-monitorChan:
			return moved, false
		default:
		}
		if !dp.pdCoord.IsClusterStable() {
			return moved, false
		}
		if !dp.pdCoord.IsMineLeader() {
			return moved, true
		}
		if dp.pdCoord.hasRemovingNode() {
			return moved, false
		}
		// balance only one namespace once
		if movedNamespace != "" && movedNamespace != namespaceInfo.Name {
			continue
		}
		if len(namespaceInfo.Removings) > 0 {
			continue
		}
		if ok, err := IsAllISRFullReady(&namespaceInfo); err != nil || !ok {
			cluster.CoordLog().Infof("namespace %v isr is not full ready while balancing", namespaceInfo.GetDesp())
			continue
		}
		currentNodes := dp.pdCoord.getCurrentNodes(namespaceInfo.Tags)
		nodeNameList := getNodeNameList(currentNodes)
		cluster.CoordLog().Debugf("node name list: %v", nodeNameList)

		partitionNodes, err := getRebalancedNamespacePartitions(
			namespaceInfo.Name,
			namespaceInfo.PartitionNum,
			namespaceInfo.Replica, currentNodes)
		if err != nil {
			isAllBalanced = false
			continue
		}
		cluster.CoordLog().Debugf("expected replicas : %v", partitionNodes)
		moveNodes := make([]string, 0)
		for _, nid := range namespaceInfo.GetISR() {
			found := false
			for _, expectedNode := range partitionNodes[namespaceInfo.Partition] {
				if nid == expectedNode {
					found = true
					break
				}
			}
			if !found {
				moveNodes = append(moveNodes, nid)
			}
		}
		for _, nid := range moveNodes {
			movedNamespace = namespaceInfo.Name
			cluster.CoordLog().Infof("node %v need move for namespace %v since %v not in expected isr list: %v", nid,
				namespaceInfo.GetDesp(), namespaceInfo.RaftNodes, partitionNodes[namespaceInfo.Partition])
			var err error
			var newInfo *cluster.PartitionMetaInfo
			if len(namespaceInfo.GetISR()) <= namespaceInfo.Replica {
				newInfo, err = dp.addNodeToNamespaceAndWaitReady(monitorChan, &namespaceInfo,
					nodeNameList)
			}
			if err != nil {
				return moved, false
			}
			if newInfo != nil {
				namespaceInfo = *newInfo
			}
			coordErr := dp.pdCoord.removeNamespaceFromNode(&namespaceInfo, nid)
			moved = true
			if coordErr != nil {
				return moved, false
			}
		}
		expectLeader := partitionNodes[namespaceInfo.Partition][0]
		if _, ok := namespaceInfo.Removings[expectLeader]; ok {
			cluster.CoordLog().Infof("namespace %v expected leader: %v is marked as removing", namespaceInfo.GetDesp(),
				expectLeader)
		} else {
			isrList := namespaceInfo.GetISR()
			if len(moveNodes) == 0 && (len(isrList) >= namespaceInfo.Replica) &&
				(namespaceInfo.RaftNodes[0] != expectLeader) {
				for index, nid := range namespaceInfo.RaftNodes {
					if nid == expectLeader {
						cluster.CoordLog().Infof("need move leader for namespace %v since %v not expected leader: %v",
							namespaceInfo.GetDesp(), namespaceInfo.RaftNodes, expectLeader)
						newNodes := make([]string, len(namespaceInfo.RaftNodes))
						copy(newNodes, namespaceInfo.RaftNodes)
						newNodes[0], newNodes[index] = newNodes[index], newNodes[0]
						namespaceInfo.RaftNodes = newNodes
						err := dp.pdCoord.register.UpdateNamespacePartReplicaInfo(namespaceInfo.Name, namespaceInfo.Partition,
							&namespaceInfo.PartitionReplicaInfo, namespaceInfo.PartitionReplicaInfo.Epoch())
						moved = true
						if err != nil {
							cluster.CoordLog().Infof("move leader for namespace %v failed: %v", namespaceInfo.GetDesp(), err)
							return moved, false
						}
					}
				}
				// wait raft leader election
				select {
				case <-monitorChan:
				case <-time.After(time.Second * 5):
				}
			}
		}
		if len(moveNodes) > 0 || moved {
			isAllBalanced = false
		}
	}

	return moved, isAllBalanced
}

type SortableStrings []string

func (s SortableStrings) Less(l, r int) bool {
	return s[l] < s[r]
}
func (s SortableStrings) Len() int {
	return len(s)
}
func (s SortableStrings) Swap(l, r int) {
	s[l], s[r] = s[r], s[l]
}

func getRebalancedNamespacePartitions(ns string,
	partitionNum int, replica int,
	currentNodes map[string]cluster.NodeInfo) ([][]string, *cluster.CoordErr) {
	if len(currentNodes) < replica {
		return nil, ErrNodeUnavailable
	}
	// for namespace we have much partitions than nodes,
	// so we need make all the nodes have the almost the same leader partitions,
	// and also we need make all the nodes have the almost the same followers.
	// and to avoid the data migration, we should keep the data as much as possible
	// algorithm as below:
	// 1. sort node id ; 2. sort the namespace partitions
	// 3. choose the (leader, follower, follower) for each partition,
	// start from the index of the current node array
	// 4. for next partition, start from the next index of node array.
	//  l -> leader, f-> follower
	//         nodeA   nodeB   nodeC   nodeD
	// p1       l       f        f
	// p2               l        f      f
	// p3       f                l      f
	// p4       f       f               l
	// p5       l       f        f
	// p6               l        f      f
	// p7       f                l      f
	// p8       f       f               l
	// p9       l       f        f
	// p10              l        f      f

	// after nodeB is down, the migration as bellow
	//         nodeA   xxxx   nodeC   nodeD
	// p1       l       x        f     x-f
	// p2      x-f      x       f-l     f
	// p3       f                l      f
	// p4       f       x       x-f     l
	// p5       l       x        f     x-f
	// p6      x-f      x       f-l     f
	// p7       f                l      f
	// p8       f       x       x-f     l
	// p9       l       x        f     x-f
	// p10     x-f      x       f-l     f

	// if there are several data centers, we sort them one by one as below
	// nodeA1@dc1 nodeA2@dc2 nodeA3@dc3 nodeB1@dc1 nodeB2@dc2 nodeB3@dc3
	// 对 @currentNodes 进行排序，排出来的效果如上所示
	nodeNameList := getNodeNameList(currentNodes)
	// 获取各partition的各replica所在的node
	return getRebalancedPartitionsFromNameList(ns, partitionNum, replica, nodeNameList)
}

// 返回值是一个二维数组，第一维是partition id，第二维则是各个replica所在的node的name
func getRebalancedPartitionsFromNameList(ns string,
	partitionNum int, replica int,
	nodeNameList []SortableStrings) ([][]string, *cluster.CoordErr) {

	// 下面这段代码似无必要，因为 getNodeNameList 已经对 @nodeNameList 进行了排序
	sortedNodeNameList := make([]SortableStrings, 0, len(nodeNameList))
	for _, nList := range nodeNameList {
		sortedNodeNameList = append(sortedNodeNameList, nList)
	}
	totalCnt := 0
	for idx, nList := range sortedNodeNameList {
		sort.Sort(nList)
		sortedNodeNameList[idx] = nList
		totalCnt += len(nList)
	}

	// 如果 node 个数小于 replica 数目，则拒绝响应请求
	if totalCnt < replica {
		return nil, ErrNodeUnavailable
	}
	idx := 0
	var combined SortableStrings // 按照 line-568 line-569 所示，循环取出各个idc的 node 组成 一个 node array
	for len(combined) < totalCnt {
		nList := sortedNodeNameList[idx%len(sortedNodeNameList)]
		if len(nList) == 0 {
			idx++
			continue
		}
		combined = append(combined, nList[0])
		sortedNodeNameList[idx%len(sortedNodeNameList)] = nList[1:]
		idx++
	}
	partitionNodes := make([][]string, partitionNum)
	selectIndex := int(murmur3.Sum32([]byte(ns))) // 初始 pos 选择为 hash(namespace) % len(combined)
	for i := 0; i < partitionNum; i++ {
		nlist := make([]string, replica)
		partitionNodes[i] = nlist
		for j := 0; j < replica; j++ {
			nlist[j] = combined[(selectIndex+j)%len(combined)]
		}
		selectIndex++
	}

	return partitionNodes, nil
}

func (dp *DataPlacement) decideUnwantedRaftNode(namespaceInfo *cluster.PartitionMetaInfo, currentNodes map[string]cluster.NodeInfo) string {
	unwantedNode := ""
	//remove the unwanted node in isr
	partitionNodes, err := getRebalancedNamespacePartitions(
		namespaceInfo.Name,
		namespaceInfo.PartitionNum,
		namespaceInfo.Replica, currentNodes)
	if err != nil {
		return unwantedNode
	}
	for _, nid := range namespaceInfo.GetISR() {
		found := false
		for _, validNode := range partitionNodes[namespaceInfo.Partition] {
			if nid == validNode {
				found = true
				break
			}
		}
		if !found {
			unwantedNode = nid
		}
	}
	return unwantedNode
}
