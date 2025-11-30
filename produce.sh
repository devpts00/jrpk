#!/usr/bin/env bash
export BUILD=$1
export FILE=$2
export TOPIC=$3
export PARTITIONS=$4

for ((p = 0; p < $PARTITIONS; p++))
do
  docker compose run --rm --remove-orphans rst ./target/${BUILD}/jrpk \
    client \
    --path=/jrpk/json/${FILE} \
    --address=jrpk:1133 \
    --topic=${TOPIC} \
    --partition=${p} \
    --max-frame-byte-size=1m \
    --metrics-uri=http://pmg:9091/metrics/job/jrpk \
    --metrics-period=1s \
    produce &
done

wait
