package node

import (
	"github.com/divebomb/exocet/raft/raftpb"
	"github.com/divebomb/exocet/snap"
	"github.com/divebomb/exocet/wal"
	"github.com/divebomb/exocet/wal/walpb"
)

type raftPersistStorage struct {
	*wal.WAL
	*snap.Snapshotter
}

func NewRaftPersistStorage(w *wal.WAL, s *snap.Snapshotter) IRaftPersistStorage {
	return &raftPersistStorage{w, s}
}

// SaveSnap saves the snapshot to disk and release the locked
// wal files since they will not be used.
func (st *raftPersistStorage) SaveSnap(snap raftpb.Snapshot) error {
	walsnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	err := st.WAL.SaveSnapshot(walsnap)
	if err != nil {
		return err
	}
	err = st.Snapshotter.SaveSnap(snap)
	if err != nil {
		return err
	}
	return st.WAL.ReleaseLockTo(snap.Metadata.Index)
}
