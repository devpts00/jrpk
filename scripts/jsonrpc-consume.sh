#!/usr/bin/env bash

export BUILD=$1
export FILE=$2
export TOPIC=$3
export PARTITIONS=$4

START_TIME=$EPOCHREALTIME

for ((p = 0; p < $PARTITIONS; p++))
do
	./target/${BUILD}/jrpk \
		client \
		--path=./json/${FILE}-${p}.json \
		--address=jrpk:1133 \
		--topic=posts \
		--partition=${p} \
		--metrics-uri=http://pmg:9091/metrics/job/jrpk \
		--metrics-period=1s \
		consume \
		--from=earliest \
		--until=latest \
		--max-batch-byte-size=128KiB \
		--max-wait-ms=50 &
	sleep 0.1
done

wait

END_TIME=$EPOCHREALTIME
DIFF_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo "Runtime: $DIFF_TIME seconds"

rm -f ./json/${FILE}-*.json
