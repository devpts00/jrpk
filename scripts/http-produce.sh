#!/usr/bin/env bash

export FILE=$1
export TOPIC=$2
export PARTITIONS=$3

for ((p = 0; p < $PARTITIONS; p++))
do
  curl -v -X POST --data-binary "@./json/${FILE}.json" "localhost:1134/kafka/send/${TOPIC}/${p}?max_batch_size=1mib" &
	#sleep 0.1
done

wait
