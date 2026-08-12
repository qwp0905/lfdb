#!/bin/bash

DEFAULT_THREAD=128
DEFAULT_RECORD=500000
DEFAULT_OP=200000
SAMPLE_SIZE=30

benchmark() {
  YCSB_RECORD_COUNT=${RECORD_COUNT:-$DEFAULT_RECORD} \
    YCSB_OP_COUNT=${OP_COUNT:-$DEFAULT_OP} \
    YCSB_THREAD_COUNT=${THREAD_COUNT:-$DEFAULT_THREAD} \
    cargo benchmark --bench ycsb \
    --features redb \
    --features sled \
    --features rocksdb -- \
    --sample-size=$SAMPLE_SIZE
}

echo "####### Default Benchmark #######"
benchmark

echo "####### Scaling Concurrency #######"
for count in {32,64,256,512}; do
  echo "####### Scaling Concurrency [$count] #######"
  THREAD_COUNT=$count benchmark
done

echo "####### Scaling Dataset #######"
for count in {1000000,2000000,5000000,10000000}; do
  echo "####### Scaling Dataset [$count] #######"
  RECORD_COUNT=$count benchmark
done

echo "####### Scaling Workload #######"
for count in {500000,1000000,2000000,5000000}; do
  echo "####### Scaling Workload [$count] #######"
  OP_COUNT=$count benchmark
done
