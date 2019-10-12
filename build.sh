#!/bin/bash

GOOS=linux
VOLANTMQ_WORK_DIR=~/mqtt
VOLANTMQ_BUILD_FLAGS="-i"
VOLANTMQ_PLUGINS_DIR=~/mqtt/plugins
GO111MODULE=off

mkdir -p $VOLANTMQ_WORK_DIR/bin
mkdir -p $VOLANTMQ_WORK_DIR/conf
mkdir -p $VOLANTMQ_PLUGINS_DIR

cd $GOPATH/src/github.com/chenyf/mqtt/cmd/broker \
    && govvv build $VOLANTMQ_BUILD_FLAGS -o $VOLANTMQ_WORK_DIR/bin/broker

# build debug plugins
cd $GOPATH/src/github.com/chenyf/mqttapi/vlplugin/debug \
    && go build $VOLANTMQ_BUILD_FLAGS -buildmode=plugin -o $VOLANTMQ_WORK_DIR/plugins/debug.so

# build health plugins
cd $GOPATH/src/github.com/chenyf/mqttapi/vlplugin/health && \
    go build $VOLANTMQ_BUILD_FLAGS -buildmode=plugin -o $VOLANTMQ_WORK_DIR/plugins/health.so

#build persistence plugins
cd $GOPATH/src/github.com/chenyf/mqttapi/vlplugin/vlpersistence/bbolt/plugin && \
    go build $VOLANTMQ_BUILD_FLAGS -buildmode=plugin -o $VOLANTMQ_WORK_DIR/plugins/persistence_bbolt.so

cd $GOPATH/src/github.com/chenyf/mqttapi/vlplugin/vlpersistence/mem/plugin && \
    go build $VOLANTMQ_BUILD_FLAGS -buildmode=plugin -o $VOLANTMQ_WORK_DIR/plugins/persistence_mem.so

cp $GOPATH/src/github.com/chenyf/mqtt/examples/config.yaml $VOLANTMQ_WORK_DIR/conf -f

