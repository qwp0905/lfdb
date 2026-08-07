#!/bin/bash

DEFAULT_THREAD=128
DEFAULT_RECORD=500000
DEFAULT_CACHE=1024
SAMPLE_SIZE=30

benchmark() {
  YCSB_RECORD_COUNT=${RECORD_COUNT:-$DEFAULT_RECORD} \
    YCSB_THREAD_COUNT=${THREAD_COUNT:-$DEFAULT_THREAD} \
    YCSB_CACHE_SIZE=${CACHE_SIZE:-$DEFAULT_CACHE} \
    cargo benchmark --bench ycsb --all-features -- \
    --sample-size=$SAMPLE_SIZE
}

echo "####### Scaling Concurrency #######"
for count in {32,64,128,256,512}; do
  echo "####### Scaling Concurrency [$count] #######"
    THREAD_COUNT=$count benchmark
done

echo "####### Scaling Dataset #######"
for count in {100000,200000,500000,1000000,2000000}; do
  echo "####### Scaling Dataset [$count] #######"
  RECORD_COUNT=$count benchmark
done

echo "####### Scaling Cache #######"
for count in {128,256,512,1024,2048}; do
  echo "####### Scaling Cache [$count] #######"
    CACHE_SIZE=$count benchmark
done
