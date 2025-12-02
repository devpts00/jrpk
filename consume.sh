#!/usr/bin/env bash

export BUILD=$1
export FILE=$2
export TOPIC=$3
export PARTITIONS=$4

for ((p = 0; p < $PARTITIONS; p++))
do
	docker compose run --rm --remove-orphans rst ./target/${BUILD}/jrpk \
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
		--max-wait-ms=10000 &
done

wait

rm -f ./json/${FILE}-*.json
