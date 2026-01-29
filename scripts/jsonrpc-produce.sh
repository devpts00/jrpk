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
      produce \
      --jrp-address jrpk:1133 \
      --jrp-frame-max-size 512kib \
      --jrp-send-max-size 128kib \
      --jrp-send-max-rec-count 1000 \
      --jrp-send-max-rec-size 1kib \
      --jrp-value-codec json \
      --kfk-topic ${TOPIC} \
      --kfk-partition ${p} \
      --file-path ./json/${FILE}.json \
      --file-format value \
      --file-load-max-size 1gib \
      --file-load-max-rec-count 1000000 \
      --prom-push-url http://pmg:9091/metrics/job/jrpk \
      --prom-push-period 1s \
      --thread-count 1 &
      sleep 0.1
  done
  wait
done

END_TIME=$EPOCHREALTIME
DIFF_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo "Runtime: $DIFF_TIME seconds"