#!/usr/bin/env bash

export FILE=$1
export TOPIC=$2
export PARTITIONS=$3
export MAX_REC_COUNT=$4
export MAX_BYTE_SIZE=$5

START_TIME=$EPOCHREALTIME

for ((p = 0; p < $PARTITIONS; p++))
do
  #curl -v --output ./json/${FILE}-${p}.json "http://jrpk:1134/kafka/fetch/posts/${p}" &
  curl -v --output ./json/${FILE}-${p}.json "http://jrpk:1134/kafka/fetch/posts/${p}" &
	sleep 0.1
done

wait

END_TIME=$EPOCHREALTIME
DIFF_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo ""
echo "Runtime: $DIFF_TIME seconds"

rm -f ./json/${FILE}-*.json
