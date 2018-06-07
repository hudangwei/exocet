package node

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/absolute8511/redcon"
	"github.com/divebomb/exocet/common"
	"github.com/divebomb/exocet/rockredis"
	"github.com/divebomb/exocet/stats"
	"github.com/divebomb/exocet/transport/rafthttp"
	"github.com/stretchr/testify/assert"
)

func getTestKVNode(t *testing.T) (*KVNode, string, chan struct{}) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("kvnode-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	t.Logf("dir:%v\n", tmpDir)
	raftAddr := "http://127.0.0.1:12345"
	var replica ReplicaInfo
	replica.NodeID = 1
	replica.ReplicaID = 1
	replica.RaftAddr = raftAddr

	if testing.Verbose() {
		rockredis.SetLogLevel(4)
		SetLogLevel(4)
	}
	ts := &stats.TransportStats{}
	ts.Initialize()
	raftTransport := &rafthttp.Transport{
		DialTimeout: time.Second * 5,
		ClusterID:   "test",
		Raft:        nil,
		Snapshotter: nil,
		TrStats:     ts,
		PeersStats:  stats.NewPeersStats(),
		ErrorC:      nil,
	}
	nsConf := NewNSConfig()
	nsConf.Name = "default-0"
	nsConf.BaseName = "default"
	nsConf.EngType = rockredis.EngType
	nsConf.PartitionNum = 1
	nsConf.Replicator = 1
	nsConf.RaftGroupConf.GroupID = 1000
	nsConf.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, replica)
	nsConf.ExpirationPolicy = "consistency_deletion"

	mconf := &MachineConfig{
		BroadcastAddr: "127.0.0.1",
		HttpAPIPort:   0,
		LocalRaftAddr: raftAddr,
		DataRootDir:   tmpDir,
		TickMs:        100,
		ElectionTick:  5,
	}
	nsMgr := NewNamespaceMgr(raftTransport, mconf)
	var kvNode *NamespaceNode
	if kvNode, err = nsMgr.InitNamespaceNode(nsConf, 1, false); err != nil {
		t.Fatalf("failed to init namespace: %v", err)
	}
	raftTransport.Raft = kvNode.Node
	raftTransport.Snapshotter = kvNode.Node

	raftTransport.Start()
	url, err := url.Parse(raftAddr)
	assert.Nil(t, err)
	stopC := make(chan struct{})
	ln, err := common.NewStoppableListener(url.Host, stopC)
	assert.Nil(t, err)
	go func() {
		(&http.Server{Handler: raftTransport.Handler()}).Serve(ln)
	}()
	nsMgr.Start()
	time.Sleep(time.Second * 3)
	return kvNode.Node, tmpDir, stopC
}

type fakeRedisConn struct {
	rsp []interface{}
	err error
}

func (c *fakeRedisConn) GetError() error { return c.err }
func (c *fakeRedisConn) Reset() {
	c.err = nil
	c.rsp = nil
}

// RemoteAddr returns the remote address of the client connection.
func (c *fakeRedisConn) RemoteAddr() string { return "" }

// Close closes the connection.
func (c *fakeRedisConn) Close() error { return nil }

// WriteError writes an error to the client.
func (c *fakeRedisConn) WriteError(msg string) { c.err = errors.New(msg) }

// WriteString writes a string to the client.
func (c *fakeRedisConn) WriteString(str string) { c.rsp = append(c.rsp, str) }

// WriteBulk writes bulk bytes to the client.
func (c *fakeRedisConn) WriteBulk(bulk []byte) { c.rsp = append(c.rsp, bulk) }

// WriteBulkString writes a bulk string to the client.
func (c *fakeRedisConn) WriteBulkString(bulk string) { c.rsp = append(c.rsp, bulk) }

// WriteInt writes an integer to the client.
func (c *fakeRedisConn) WriteInt(num int) { c.rsp = append(c.rsp, num) }

// WriteInt64 writes a 64-but signed integer to the client.
func (c *fakeRedisConn) WriteInt64(num int64) { c.rsp = append(c.rsp, num) }

func (c *fakeRedisConn) WriteArray(count int) { c.rsp = append(c.rsp, count) }

// WriteNull writes a null to the client
func (c *fakeRedisConn) WriteNull() { c.rsp = append(c.rsp, nil) }

// WriteRaw writes raw data to the client.
func (c *fakeRedisConn) WriteRaw(data []byte) { c.rsp = append(c.rsp, data) }

// Context returns a user-defined context
func (c *fakeRedisConn) Context() interface{} { return nil }

// SetContext sets a user-defined context
func (c *fakeRedisConn) SetContext(v interface{}) {}

// SetReadBuffer updates the buffer read size for the connection
func (c *fakeRedisConn) SetReadBuffer(bytes int) {}

func (c *fakeRedisConn) Detach() redcon.DetachedConn { return nil }

func (c *fakeRedisConn) ReadPipeline() []redcon.Command { return nil }

func (c *fakeRedisConn) PeekPipeline() []redcon.Command { return nil }
func (c *fakeRedisConn) NetConn() net.Conn              { return nil }

func TestKVNode_kvCommand(t *testing.T) {
	nd, dataDir, stopC := getTestKVNode(t)
	testKey := []byte("default:test:1")
	testKeyValue := []byte("1")
	testKey2 := []byte("default:test:2")
	testKey2Value := []byte("2")
	testPFKey := []byte("default:test:pf1")
	tests := []struct {
		name string
		args redcon.Command
	}{
		{"get", buildCommand([][]byte{[]byte("get"), testKey})},
		{"mget", buildCommand([][]byte{[]byte("mget"), testKey, testKey2})},
		{"exists", buildCommand([][]byte{[]byte("exists"), testKey, testKey2})},
		{"set", buildCommand([][]byte{[]byte("set"), testKey, testKeyValue})},
		{"setnx", buildCommand([][]byte{[]byte("setnx"), testKey, testKeyValue})},
		{"setnx", buildCommand([][]byte{[]byte("setnx"), testKey2, testKey2Value})},
		//{"mset", buildCommand([][]byte{[]byte("mset"), testKey, testKeyValue, testKey2, testKey2Value})},
		{"del", buildCommand([][]byte{[]byte("del"), testKey, testKey2})},
		{"incr", buildCommand([][]byte{[]byte("incr"), testKey})},
		{"incrby", buildCommand([][]byte{[]byte("incrby"), testKey, testKey2Value})},
		{"get", buildCommand([][]byte{[]byte("get"), testKey})},
		{"mget", buildCommand([][]byte{[]byte("mget"), testKey, testKey2})},
		{"exists", buildCommand([][]byte{[]byte("exists"), testKey})},
		{"pfadd", buildCommand([][]byte{[]byte("pfadd"), testPFKey, testKeyValue})},
		{"pfcount", buildCommand([][]byte{[]byte("pfcount"), testPFKey})},
	}
	defer os.RemoveAll(dataDir)
	defer nd.Stop()
	defer close(stopC)
	c := &fakeRedisConn{}
	for _, cmd := range tests {
		c.Reset()
		handler, _, _ := nd.router.GetCmdHandler(cmd.name)
		if handler != nil {
			handler(c, cmd.args)
		} else {
			handler, _, _ := nd.router.GetMergeCmdHandler(cmd.name)
			_, err := handler(cmd.args)
			assert.Nil(t, err)
		}
		assert.Nil(t, c.GetError())
	}
}
