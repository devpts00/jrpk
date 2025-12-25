#!/usr/bin/env bash

export FILE=$1
export TOPIC=$2
export PARTITIONS=$3
export MAX_REC_COUNT=$4
export MAX_BYTE_SIZE=$5

for ((p = 0; p < $PARTITIONS; p++))
do
  curl -v --output ./json/${FILE}-${p}.json "localhost:9999/kafka/fetch/posts/${p}?from=earliest&until=latest&max_rec_count=${MAX_REC_COUNT}&max_bytes_size=${MAX_BYTE_SIZE}" &
	sleep 0.1
done

wait

rm -f ./json/${FILE}-*.json
