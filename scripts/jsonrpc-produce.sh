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
    --path ./json/${FILE}.json \
    --address jrpk:1133 \
    --topic ${TOPIC} \
    --partition ${p} \
    --max-frame-byte-size 32kib \
    --metrics-uri http://pmg:9091/metrics/job/jrpk \
    --metrics-period 1s \
    --format value \
    --header-codecs abc:json,xyz:base64 \
    --key-codec str \
    --value-codec base64 \
    produce \
    --max-batch-byte-size 16kib \
    --max-rec-byte-size 2kib &
    sleep 0.1
done

wait

END_TIME=$EPOCHREALTIME
DIFF_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo "Runtime: $DIFF_TIME seconds"