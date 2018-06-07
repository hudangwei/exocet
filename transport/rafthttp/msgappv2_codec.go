// Copyright 2015 The etcd Authors
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

package rafthttp

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/divebomb/exocet/pkg/pbutil"
	"github.com/divebomb/exocet/pkg/types"
	"github.com/divebomb/exocet/raft/raftpb"
	"github.com/divebomb/exocet/stats"
)

const (
	msgTypeLinkHeartbeat uint8 = 0
	msgTypeAppEntries    uint8 = 1
	msgTypeApp           uint8 = 2

	msgAppV2BufSize = 1024 * 1024
)

// msgappv2 stream sends three types of message: linkHeartbeatMessage,
// AppEntries and MsgApp. AppEntries is the MsgApp that is sent in
// replicate state in raft, whose index and term are fully predictable.
//
// Data format of linkHeartbeatMessage:
// | offset | bytes | description |
// +--------+-------+-------------+
// | 0      | 1     | \x00        |
//
// Data format of AppEntries:
// | offset | bytes | description |
// +--------+-------+-------------+
// | 0      | 1     | \x01        |
// | 1      | 8     | length of entries |
// | 9      | 8     | length of first entry |
// | 17     | n1    | first entry |
// ...
// | x      | 8     | length of k-th entry data |
// | x+8    | nk    | k-th entry data |
// | x+8+nk | 8     | commit index |
//
// Data format of MsgApp:
// | offset | bytes | description |
// +--------+-------+-------------+
// | 0      | 1     | \x02        |
// | 1      | 8     | length of encoded message |
// | 9      | n     | encoded message |
type msgAppV2Encoder struct {
	w  io.Writer
	ps *stats.PeerStats

	term      uint64
	index     uint64
	ToGroup   raftpb.Group
	FromGroup raftpb.Group
	buf       []byte
	uint64buf []byte
	uint8buf  []byte
}

func isSameGroup(l *raftpb.Group, r *raftpb.Group) bool {
	return l.NodeId == r.NodeId && l.GroupId == r.GroupId &&
		l.RaftReplicaId == r.RaftReplicaId
}

func newMsgAppV2Encoder(w io.Writer, ps *stats.PeerStats) *msgAppV2Encoder {
	return &msgAppV2Encoder{
		w:         w,
		ps:        ps,
		buf:       make([]byte, msgAppV2BufSize),
		uint64buf: make([]byte, 8),
		uint8buf:  make([]byte, 1),
	}
}

func (enc *msgAppV2Encoder) isContinue(m *raftpb.Message) bool {
	return enc.index == m.Index && enc.term == m.LogTerm && m.LogTerm == m.Term &&
		isSameGroup(&enc.ToGroup, &m.ToGroup) && isSameGroup(&enc.FromGroup, &m.FromGroup)
}

func (enc *msgAppV2Encoder) encode(m *raftpb.Message) error {
	start := time.Now()
	switch {
	case isLinkHeartbeatMessage(m):
		enc.uint8buf[0] = byte(msgTypeLinkHeartbeat)
		if _, err := enc.w.Write(enc.uint8buf); err != nil {
			return err
		}
	case enc.isContinue(m):
		enc.uint8buf[0] = byte(msgTypeAppEntries)
		if _, err := enc.w.Write(enc.uint8buf); err != nil {
			return err
		}
		// write length of entries
		binary.BigEndian.PutUint64(enc.uint64buf, uint64(len(m.Entries)))
		if _, err := enc.w.Write(enc.uint64buf); err != nil {
			return err
		}
		for i := 0; i < len(m.Entries); i++ {
			// write length of entry
			binary.BigEndian.PutUint64(enc.uint64buf, uint64(m.Entries[i].Size()))
			if _, err := enc.w.Write(enc.uint64buf); err != nil {
				return err
			}
			if n := m.Entries[i].Size(); n < msgAppV2BufSize {
				if _, err := m.Entries[i].MarshalTo(enc.buf[:n]); err != nil {
					return err
				}
				if _, err := enc.w.Write(enc.buf[:n]); err != nil {
					return err
				}
			} else {
				if _, err := enc.w.Write(pbutil.MustMarshal(&m.Entries[i])); err != nil {
					return err
				}
			}
			enc.index++
		}
		// write commit index
		binary.BigEndian.PutUint64(enc.uint64buf, m.Commit)
		if _, err := enc.w.Write(enc.uint64buf); err != nil {
			return err
		}
		enc.ps.Succ(time.Since(start))
	default:
		if err := binary.Write(enc.w, binary.BigEndian, msgTypeApp); err != nil {
			return err
		}
		// write size of message
		if err := binary.Write(enc.w, binary.BigEndian, uint64(m.Size())); err != nil {
			return err
		}
		// write message
		if _, err := enc.w.Write(pbutil.MustMarshal(m)); err != nil {
			return err
		}

		enc.term = m.Term
		enc.index = m.Index
		enc.ToGroup = m.ToGroup
		enc.FromGroup = m.FromGroup
		if l := len(m.Entries); l > 0 {
			enc.index = m.Entries[l-1].Index
		}
		enc.ps.Succ(time.Since(start))
	}
	return nil
}

type msgAppV2Decoder struct {
	r             io.Reader
	local, remote types.ID

	term      uint64
	index     uint64
	ToGroup   raftpb.Group
	FromGroup raftpb.Group
	buf       []byte
	uint64buf []byte
	uint8buf  []byte
}

func newMsgAppV2Decoder(r io.Reader, local, remote types.ID) *msgAppV2Decoder {
	return &msgAppV2Decoder{
		r:         r,
		local:     local,
		remote:    remote,
		buf:       make([]byte, msgAppV2BufSize),
		uint64buf: make([]byte, 8),
		uint8buf:  make([]byte, 1),
	}
}

func (dec *msgAppV2Decoder) decode() (raftpb.Message, error) {
	var (
		m   raftpb.Message
		typ uint8
	)
	if _, err := io.ReadFull(dec.r, dec.uint8buf); err != nil {
		return m, err
	}
	typ = uint8(dec.uint8buf[0])
	switch typ {
	case msgTypeLinkHeartbeat:
		return linkHeartbeatMessage, nil
	case msgTypeAppEntries:
		if uint64(dec.remote) != dec.FromGroup.NodeId ||
			uint64(dec.local) != dec.ToGroup.NodeId {
			return m, fmt.Errorf("remote node and local node maybe not matched: %v, %v (%v, %v)",
				dec.remote, dec.local, dec.FromGroup, dec.ToGroup)
		}
		m = raftpb.Message{
			Type:      raftpb.MsgApp,
			From:      dec.FromGroup.RaftReplicaId,
			To:        dec.ToGroup.RaftReplicaId,
			Term:      dec.term,
			LogTerm:   dec.term,
			ToGroup:   dec.ToGroup,
			FromGroup: dec.FromGroup,
			Index:     dec.index,
		}

		// decode entries
		if _, err := io.ReadFull(dec.r, dec.uint64buf); err != nil {
			return m, err
		}
		l := binary.BigEndian.Uint64(dec.uint64buf)
		m.Entries = make([]raftpb.Entry, int(l))
		for i := 0; i < int(l); i++ {
			if _, err := io.ReadFull(dec.r, dec.uint64buf); err != nil {
				return m, err
			}
			size := binary.BigEndian.Uint64(dec.uint64buf)
			var buf []byte
			if size < msgAppV2BufSize {
				buf = dec.buf[:size]
				if _, err := io.ReadFull(dec.r, buf); err != nil {
					return m, err
				}
			} else {
				buf = make([]byte, int(size))
				if _, err := io.ReadFull(dec.r, buf); err != nil {
					return m, err
				}
			}
			dec.index++
			// 1 alloc
			err := pbutil.MaybeUnmarshal(&m.Entries[i], buf)
			if err != nil {
				return m, err
			}
		}
		// decode commit index
		if _, err := io.ReadFull(dec.r, dec.uint64buf); err != nil {
			return m, err
		}
		m.Commit = binary.BigEndian.Uint64(dec.uint64buf)
	case msgTypeApp:
		var size uint64
		if err := binary.Read(dec.r, binary.BigEndian, &size); err != nil {
			return m, err
		}
		buf := make([]byte, int(size))
		if _, err := io.ReadFull(dec.r, buf); err != nil {
			return m, err
		}
		err := pbutil.MaybeUnmarshal(&m, buf)
		if err != nil {
			return m, err
		}

		dec.term = m.Term
		dec.index = m.Index
		dec.FromGroup = m.FromGroup
		dec.ToGroup = m.ToGroup
		if l := len(m.Entries); l > 0 {
			dec.index = m.Entries[l-1].Index
		}
	default:
		return m, fmt.Errorf("failed to parse type %d in msgappv2 stream", typ)
	}
	return m, nil
}
