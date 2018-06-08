#!/usr/bin/env bash
# ******************************************************
# DESC    : rocksdb build script
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : Apache License 2.0
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2018-05-18 23:17
# FILE    : build.sh
# ******************************************************

# make static_lib -e DISABLE_JEMALLOC=1 -j8

make static_lib -j8
