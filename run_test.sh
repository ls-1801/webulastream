#!/bin/bash

FILE_NAME=$1
NUMBER_OF_NODES=$(yq '. | length' $FILE_NAME)
NUMBER_OF_NODES=$(($NUMBER_OF_NODES - 1))
cat $FILE_NAME
echo "Starting $NUMBER_OF_NODES nodes"

cargo build
for i in $(seq 0 $NUMBER_OF_NODES); do
  cargo run -- $FILE_NAME $i &
done

wait