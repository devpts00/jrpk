#!/usr/bin/env bash
export TOPIC=$1
export PARTITION=$2
export OFFSET=$3
curl -v "http://jrpk:1134/kafka/offset/${TOPIC}/${PARTITION}?kfk_offset=${OFFSET}"