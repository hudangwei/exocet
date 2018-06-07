package node

import (
	"errors"

	"github.com/absolute8511/redcon"
	"github.com/divebomb/exocet/common"
)

func (nd *KVNode) plsetCommand(cmd redcon.Command, rsp interface{}) (interface{}, error) {
	return rsp, nil
}

func (kvsm *kvStoreSM) localPlsetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if len(cmd.Args) < 3 || (len(cmd.Args)-1)%2 != 0 {
		return nil, errors.New("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
	}

	var kvpairs []common.KVRecord
	for i := 1; i < len(cmd.Args); i += 2 {
		kvpairs = append(kvpairs, common.KVRecord{Key: cmd.Args[i], Value: cmd.Args[i+1]})
	}
	err := kvsm.store.MSet(ts, kvpairs...)
	return nil, err
}
