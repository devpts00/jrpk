#!/usr/bin/env bash

export BUILD=$1
export FILE=$2
export TOPIC=$3
export PARTITIONS=$4
export COUNT=$5

START_TIME=$EPOCHREALTIME

for ((i = 0; i < $COUNT; i++))
do
  for ((p = 0; p < $PARTITIONS; p++))
  do
    ./target/${BUILD}/jrpk \
      consume \
      --jrp-address=jrpk:1133 \
      --jrp-frame-max-size=4mib \
      --jrp-key-codec=str \
      --jrp-value-codec=json \
      --jrp-header-codec-default=str \
      --kfk-topic=posts \
      --kfk-partition=${p} \
      --kfk-offset-from=earliest \
      --kfk-offset-until=latest \
      --kfk-fetch-min-size=32kib \
      --kfk-fetch-max-size=1mib \
      --kfk-fetch-max-wait-time-ms=1 \
      --file-path=./json/${FILE}-${p}.json \
      --file-format=record \
      --file-save-max-size=32mib \
      --file-save-max-rec-count=1000000000 \
      --prom-push-url=http://pmg:9091/metrics/job/jrpk \
      --prom-push-period=1s \
      --thread-count=1 &
    sleep 0.1
  done
  wait
  rm -f ./json/${FILE}-*.json
done

END_TIME=$EPOCHREALTIME
DIFF_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo "Runtime: $DIFF_TIME seconds"
