package cluster

import (
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"golang.org/x/net/context"
)

// A watch only tells the latest revision
type Watch struct {
	// Last seen revision
	revision int64
	// A channel to wait, will be closed after revision changes
	cond chan struct{}
	// Use RWMutex to protect cond variable
	rwl sync.RWMutex
}

// Wait until revision is greater than lastRevision
func (w *Watch) WaitNext(ctx context.Context, lastRevision int64) (int64, error) {
	for {
		w.rwl.RLock()
		if w.revision > lastRevision {
			w.rwl.RUnlock()
			break
		}
		cond := w.cond
		w.rwl.RUnlock()
		select {
		case <-cond:
		case <-ctx.Done():
			return 0, context.Canceled
		}
	}
	return w.revision, nil
}

// Update revision
func (w *Watch) update(newRevision int64) {
	w.rwl.Lock()
	defer w.rwl.Unlock()
	w.revision = newRevision
	close(w.cond)
	w.cond = make(chan struct{})
}

func createWatch(client *clientv3.Client, prefix string) *Watch {
	w := &Watch{0, make(chan struct{}), sync.RWMutex{}}
	go func() {
		rch := client.Watch(context.Background(), prefix, clientv3.WithPrefix(),
			clientv3.WithCreatedNotify())
		coordLog.Debugf("Watch created on %s", prefix)
		for {
			for wresp := range rch {
				if wresp.CompactRevision > w.revision {
					// respect CompactRevision
					w.update(wresp.CompactRevision)
					coordLog.Debugf("Watch to '%s' updated to %d by CompactRevision", prefix, wresp.CompactRevision)
				} else if wresp.Header.GetRevision() > w.revision {
					// Watch created or updated
					w.update(wresp.Header.GetRevision())
					coordLog.Debugf("Watch to '%s' updated to %d by header revision", prefix, wresp.Header.GetRevision())
				}
				if err := wresp.Err(); err != nil {
					coordLog.Errorf("Watch error: %s", err.Error())
				}
			}
			coordLog.Warningf("Watch to '%s' stopped at revision %d", prefix, w.revision)
			// Disconnected or cancelled
			// Wait for a moment to avoid reconnecting
			// too quickly
			time.Sleep(time.Duration(1) * time.Second)
			// Start from next revision so we are not missing anything
			if w.revision > 0 {
				rch = client.Watch(context.Background(), prefix, clientv3.WithPrefix(),
					clientv3.WithRev(w.revision+1))
			} else {
				// Start from the latest revision
				rch = client.Watch(context.Background(), prefix, clientv3.WithPrefix(),
					clientv3.WithCreatedNotify())
			}
		}
	}()
	return w
}

func register(client *clientv3.Client, k, v string, ttl int64, stopC <-chan bool) {
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)
	var curLeaseID clientv3.LeaseID = 0

	for {
		select {
		case <-stopC:
			client.Revoke(context.Background(), curLeaseID)
			return
		default:
		}
		if curLeaseID == 0 {
			leaseResp, err := lease.Grant(context.TODO(), ttl)
			if err != nil {
				goto SLEEP
			}
			if _, err := kv.Put(context.TODO(), k, v, clientv3.WithLease(leaseResp.ID)); err != nil {
				goto SLEEP
			}
			curLeaseID = leaseResp.ID
		} else {
			if _, err := lease.KeepAliveOnce(context.TODO(), curLeaseID); err == rpctypes.ErrLeaseNotFound {
				curLeaseID = 0
				continue
			}
		}
	SLEEP:
		time.Sleep(time.Duration(1) * time.Second)
	}
}

func kvGet(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), kvRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(c).Get(ctx, key, opts...)
	if err != nil {
		coordLog.Errorf("kv get key %v from etcd error: %v", key, err)
		return nil, err
	}
	if cost := time.Since(start); cost > kvSlowRequestTime {
		coordLog.Warningf("kv gets too slow: key %v cost %v err %v", key, cost, err)
	}
	if resp == nil || resp.Kvs == nil {
		return nil, ErrNoKV
	}

	return resp, err
}

func kvCompareAndSwap(c *clientv3.Client, key, value string, revision int64) (*clientv3.TxnResponse, error) {
	t := c.Txn(context.Background()).If(clientv3.Compare(clientv3.CreateRevision(key), "=", revision))
	t = t.Then(clientv3.OpPut(key, value))
	return t.Commit()
}

func kvCompareAndDelete(c *clientv3.Client, key string, revision int64) (*clientv3.TxnResponse, error) {
	t := c.Txn(context.Background()).If(clientv3.Compare(clientv3.CreateRevision(key), "=", revision))
	t = t.Then(clientv3.OpDelete(key))
	return t.Commit()
}

func kvSave(c *clientv3.Client, key, value string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), kvRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(c).Put(ctx, key, value, opts...)
	if err != nil {
		coordLog.Errorf("kv put key %v value %v to etcd error: %v", key, value, err)
		return nil, err
	}
	if cost := time.Since(start); cost > kvSlowRequestTime {
		coordLog.Warningf("kv put too slow: key %v value %v cost %v err %v", key, value, cost, err)
	}
	return resp, nil
}

func kvDel(c *clientv3.Client, key string, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(c.Ctx(), kvRequestTimeout)
	defer cancel()

	start := time.Now()
	_, err := clientv3.NewKV(c).Delete(ctx, key, opts...)
	if err != nil {
		coordLog.Errorf("kv delete key %v from etcd error: %v", key, err)
		return err
	}
	if cost := time.Since(start); cost > kvSlowRequestTime {
		coordLog.Warningf("kv delete too slow: key %v cost %v err %v", key, cost, err)
	}
	return nil
}
