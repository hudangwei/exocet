package node

import (
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/absolute8511/redcon"
	"github.com/divebomb/exocet/common"
)

var nodeLog = common.NewLevelLogger(common.LOG_INFO, common.NewDefaultLogger("node"))
var syncerOnly int32

func SetLogLevel(level int) {
	nodeLog.SetLevel(int32(level))
}

func SetLogger(level int32, logger common.Logger) {
	nodeLog.SetLevel(level)
	nodeLog.Logger = logger
}

func SetSyncerOnly(enable bool) {
	if enable {
		atomic.StoreInt32(&syncerOnly, 1)
	} else {
		atomic.StoreInt32(&syncerOnly, 0)
	}
}

func IsSyncerOnly() bool {
	return atomic.LoadInt32(&syncerOnly) == 1
}

func buildCommand(args [][]byte) redcon.Command {
	// build a pipeline command
	buf := make([]byte, 0, 512)
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(len(args)), 10)...)
	buf = append(buf, '\r', '\n')

	poss := make([]int, 0, len(args)*2)
	for _, arg := range args {
		buf = append(buf, '$')
		buf = append(buf, strconv.FormatInt(int64(len(arg)), 10)...)
		buf = append(buf, '\r', '\n')
		poss = append(poss, len(buf), len(buf)+len(arg))
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}

	// reformat a new command
	var ncmd redcon.Command
	ncmd.Raw = buf
	ncmd.Args = make([][]byte, len(poss)/2)
	for i, j := 0, 0; i < len(poss); i, j = i+2, j+1 {
		ncmd.Args[j] = ncmd.Raw[poss[i]:poss[i+1]]
	}
	return ncmd
}

func rebuildFirstKeyAndPropose(kvn *KVNode, conn redcon.Conn, cmd redcon.Command) (redcon.Command,
	interface{}, bool) {
	_, key, err := common.ExtractNamesapce(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return cmd, nil, false
	}

	if common.IsValidTableName(key) {
		conn.WriteError(common.ErrInvalidTableName.Error())
		return cmd, nil, false
	}

	cmd.Args[1] = key
	ncmd := buildCommand(cmd.Args)
	copy(cmd.Raw[0:], ncmd.Raw[:])
	cmd.Raw = cmd.Raw[:len(ncmd.Raw)]
	rsp, err := kvn.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return cmd, nil, false
	}
	return cmd, rsp, true
}

func wrapReadCommandK(f common.CommandFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, key, err := common.ExtractNamesapce(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		cmd.Args[1] = key
		f(conn, cmd)
	}
}

func wrapReadCommandKSubkey(f common.CommandFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, key, err := common.ExtractNamesapce(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		cmd.Args[1] = key
		f(conn, cmd)
	}
}

func wrapReadCommandKSubkeySubkey(f common.CommandFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, key, err := common.ExtractNamesapce(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		cmd.Args[1] = key
		f(conn, cmd)
	}
}

func wrapReadCommandKAnySubkey(f common.CommandFunc) common.CommandFunc {
	return wrapReadCommandKAnySubkeyN(f, 0)
}

func wrapReadCommandKAnySubkeyN(f common.CommandFunc, minSubLen int) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 2+minSubLen {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, key, err := common.ExtractNamesapce(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		cmd.Args[1] = key
		f(conn, cmd)
	}
}

func wrapReadCommandKK(f common.CommandFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		if len(cmd.Args[1:]) >= common.MAX_BATCH_NUM {
			conn.WriteError(errTooMuchBatchSize.Error())
			return
		}
		for i := 1; i < len(cmd.Args); i++ {
			_, key, err := common.ExtractNamesapce(cmd.Args[i])
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			cmd.Args[i] = key
		}
		f(conn, cmd)
	}
}

func wrapWriteCommandK(kvn *KVNode, f common.CommandRspFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		cmd, rsp, ok := rebuildFirstKeyAndPropose(kvn, conn, cmd)
		if !ok {
			return
		}
		f(conn, cmd, rsp)
	}
}

func wrapWriteCommandKK(kvn *KVNode, f common.CommandRspFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		args := cmd.Args[1:]
		if len(args) >= common.MAX_BATCH_NUM {
			conn.WriteError(errTooMuchBatchSize.Error())
			return
		}
		for i, v := range args {
			_, key, err := common.ExtractNamesapce(v)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			if common.IsValidTableName(key) {
				conn.WriteError(common.ErrInvalidTableName.Error())
				return
			}

			args[i] = key
		}
		ncmd := buildCommand(cmd.Args)
		copy(cmd.Raw[0:], ncmd.Raw[:])
		cmd.Raw = cmd.Raw[:len(ncmd.Raw)]

		rsp, err := kvn.Propose(cmd.Raw)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}

		f(conn, cmd, rsp)
	}
}

func wrapWriteCommandKSubkey(kvn *KVNode, f common.CommandRspFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		cmd, rsp, ok := rebuildFirstKeyAndPropose(kvn, conn, cmd)
		if !ok {
			return
		}
		f(conn, cmd, rsp)
	}
}

func wrapWriteCommandKSubkeySubkey(kvn *KVNode, f common.CommandRspFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		cmd, rsp, ok := rebuildFirstKeyAndPropose(kvn, conn, cmd)
		if !ok {
			return
		}
		f(conn, cmd, rsp)
	}
}

func wrapWriteCommandKAnySubkey(kvn *KVNode, f common.CommandRspFunc, minSubKeyLen int) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 2+minSubKeyLen {
			conn.WriteError("ERR wrong number arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		cmd, rsp, ok := rebuildFirstKeyAndPropose(kvn, conn, cmd)
		if !ok {
			return
		}
		f(conn, cmd, rsp)
	}
}

func wrapWriteCommandKV(kvn *KVNode, f common.CommandRspFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		cmd, rsp, ok := rebuildFirstKeyAndPropose(kvn, conn, cmd)
		if !ok {
			return
		}
		f(conn, cmd, rsp)
	}
}

func wrapWriteCommandKVV(kvn *KVNode, f common.CommandRspFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR wrong number arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		cmd, rsp, ok := rebuildFirstKeyAndPropose(kvn, conn, cmd)
		if !ok {
			return
		}
		f(conn, cmd, rsp)
	}
}

func wrapWriteCommandKVKV(kvn *KVNode, f common.CommandRspFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 3 || len(cmd.Args[1:])%2 != 0 {
			conn.WriteError("ERR wrong number arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		if len(cmd.Args[1:])/2 >= common.MAX_BATCH_NUM {
			conn.WriteError(errTooMuchBatchSize.Error())
			return
		}
		args := cmd.Args[1:]
		for i, v := range args {
			if i%2 != 0 {
				continue
			}
			_, key, err := common.ExtractNamesapce(v)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			if common.IsValidTableName(key) {
				conn.WriteError(common.ErrInvalidTableName.Error())
				return
			}

			args[i] = key
		}
		ncmd := buildCommand(cmd.Args)
		copy(cmd.Raw[0:], ncmd.Raw[:])
		cmd.Raw = cmd.Raw[:len(ncmd.Raw)]

		rsp, err := kvn.Propose(cmd.Raw)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		f(conn, cmd, rsp)
	}
}

func wrapWriteCommandKSubkeyV(kvn *KVNode, f common.CommandRspFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) != 4 {
			conn.WriteError("ERR wrong number arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		cmd, rsp, ok := rebuildFirstKeyAndPropose(kvn, conn, cmd)
		if !ok {
			return
		}
		f(conn, cmd, rsp)
	}
}

func wrapWriteCommandKSubkeyVSubkeyV(kvn *KVNode, f common.CommandRspFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 4 || len(cmd.Args[2:])%2 != 0 {
			conn.WriteError("ERR wrong number arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		if len(cmd.Args[2:])/2 >= common.MAX_BATCH_NUM {
			conn.WriteError(errTooMuchBatchSize.Error())
			return
		}
		cmd, rsp, ok := rebuildFirstKeyAndPropose(kvn, conn, cmd)
		if !ok {
			return
		}
		f(conn, cmd, rsp)
	}
}

func wrapMergeCommand(f common.MergeCommandFunc) common.MergeCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		_, key, err := common.ExtractNamesapce(cmd.Args[1])
		if err != nil {
			return nil, err
		}
		cmd.Args[1] = key

		return f(cmd)
	}
}

func wrapMergeCommandKK(f common.MergeCommandFunc) common.MergeCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) < 2 {
			return nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", string(cmd.Args[0]))
		}
		if len(cmd.Args[1:]) >= common.MAX_BATCH_NUM {
			return nil, errTooMuchBatchSize
		}
		for i := 1; i < len(cmd.Args); i++ {
			_, key, err := common.ExtractNamesapce(cmd.Args[i])
			if err != nil {
				return nil, err
			}
			cmd.Args[i] = key
		}
		return f(cmd)
	}
}

func wrapWriteMergeCommandKK(kvn *KVNode, f common.MergeWriteCommandFunc) common.MergeCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) < 2 {
			return nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", string(cmd.Args[0]))
		}
		args := cmd.Args[1:]
		if len(args) >= common.MAX_BATCH_NUM {
			return nil, errTooMuchBatchSize
		}
		for i, v := range args {
			_, key, err := common.ExtractNamesapce(v)
			if err != nil {
				return nil, err
			}
			if common.IsValidTableName(key) {
				return nil, common.ErrInvalidTableName
			}
			args[i] = key
		}
		ncmd := buildCommand(cmd.Args)
		copy(cmd.Raw[0:], ncmd.Raw[:])
		cmd.Raw = cmd.Raw[:len(ncmd.Raw)]

		rsp, err := kvn.Propose(cmd.Raw)
		if err != nil {
			return nil, err
		}

		return f(cmd, rsp)
	}
}

func wrapWriteMergeCommandKVKV(kvn *KVNode, f common.MergeWriteCommandFunc) common.MergeCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) < 3 || len(cmd.Args[1:])%2 != 0 {
			return nil, fmt.Errorf("ERR wrong number arguments for '%s' command", string(cmd.Args[0]))
		}
		if len(cmd.Args[1:])/2 >= common.MAX_BATCH_NUM {
			return nil, errTooMuchBatchSize
		}
		args := cmd.Args[1:]
		for i, v := range args {
			if i%2 != 0 {
				continue
			}
			_, key, err := common.ExtractNamesapce(v)
			if err != nil {
				return nil, err
			}
			if common.IsValidTableName(key) {
				return nil, common.ErrInvalidTableName
			}
			args[i] = key
		}
		ncmd := buildCommand(cmd.Args)
		copy(cmd.Raw[0:], ncmd.Raw[:])
		cmd.Raw = cmd.Raw[:len(ncmd.Raw)]

		rsp, err := kvn.Propose(cmd.Raw)
		if err != nil {
			return nil, err
		}
		return f(cmd, rsp)
	}
}
