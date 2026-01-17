#!/usr/bin/env bash

export BUILD=$1
export FILE=$2
export TOPIC=$3
export PARTITIONS=$4

START_TIME=$EPOCHREALTIME

for ((i = 0; i < 1; i++))
do
  for ((p = 0; p < $PARTITIONS; p++))
  do
    ./target/${BUILD}/jrpk \
      consume \
      --path=./json/${FILE}-${p}.json \
      --address=jrpk:1133 \
      --topic=posts \
      --partition=${p} \
      --metrics-url=http://pmg:9091/metrics/job/jrpk \
      --metrics-period=1s \
      --from=earliest \
      --until=latest \
      --max-batch-byte-size=32KiB \
      --max-wait-ms=50 \
      --file-format=value \
      --value-codec=json &
#      --format=record \
#      --key=str \
#      --value=json \
#      --header-default=str &
    sleep 0.1
  done
  wait
  rm -f ./json/${FILE}-*.json
done

END_TIME=$EPOCHREALTIME
DIFF_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo "Runtime: $DIFF_TIME seconds"
