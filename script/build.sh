#!/usr/bin/env bash
# ******************************************************
# DESC    : exocet build script
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : Apache License 2.0
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2018-05-18 23:24
# FILE    : build.sh
# ******************************************************


export GOOS=darwin
export GOARCH=amd64

PROJECT_DIR=`pwd`

rm -rf ./bin/
mkdir -p bin

cd ${PROJECT_DIR}/apps/placedriver &&  GOOS=$GOOS GOARCH=$GOARCH go build -o ${PROJECT_DIR}/bin/placedriver && cd ${PROJECT_DIR}
cd ${PROJECT_DIR}/apps/backup && GOOS=$GOOS GOARCH=$GOARCH go build -o ${PROJECT_DIR}/bin/backup && cd ${PROJECT_DIR}
cd ${PROJECT_DIR}/apps/restore && GOOS=$GOOS GOARCH=$GOARCH go build -o ${PROJECT_DIR}/bin/restore && cd ${PROJECT_DIR}
cd ${PROJECT_DIR}/apps/zankv && GOOS=$GOOS GOARCH=$GOARCH CGO_CFLAGS="-I/Users/alex/test/golang/lib/src/github.com/AlexStocks/eel/rocksdb/include" CGO_LDFLAGS="-L/Users/alex/test/golang/lib/src/github.com/AlexStocks/eel/rocksdb/lib -lrocksdb -lstdc++ -lm -lsnappy" go build -o ${PROJECT_DIR}/bin/zankv  && cd ${PROJECT_DIR}
