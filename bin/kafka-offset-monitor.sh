#!/usr/bin/env bash

CUR=$(cd `dirname $0`; pwd)
CUR=`dirname $CUR`

[ `test -d $CUR/logs` ] || mkdir $CUR/logs

MAIN_CLASS=com.sw.kafka.offsetmonitor.Main
JVM_ARGS="-Xloggc:$CUR/logs/gc.log -XX:+PrintGCDateStamps -XX:+PrintGCDetails"

exec java $JVM_ARGS -cp "$CUR/libs/*:$CUR/conf" $MAIN_CLASS