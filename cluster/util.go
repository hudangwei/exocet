package cluster

import (
	"github.com/divebomb/exocet/common"
)

var coordLog = common.NewLevelLogger(common.LOG_INFO, common.NewDefaultLogger("cluster"))

func CoordLog() *common.LevelLogger {
	return coordLog
}

func SetLogLevel(level int) {
	coordLog.SetLevel(int32(level))
}

func SetLogger(level int32, logger common.Logger) {
	coordLog.SetLevel(level)
	coordLog.Logger = logger
}
