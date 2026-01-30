#!/usr/bin/env bash

export FILE=$1
export TOPIC=$2
export PARTITIONS=$3

START_TIME=$EPOCHREALTIME

for ((p = 0; p < $PARTITIONS; p++))
do
  curl -v -X POST --data-binary "@./json/${FILE}.json" "http://jrpk:1134/kafka/send/${TOPIC}/${p}" &
	sleep 0.1
done

wait

END_TIME=$EPOCHREALTIME
DIFF_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo ""
echo "Runtime: $DIFF_TIME seconds"
