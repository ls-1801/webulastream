#! /bin/bash

FILE_NAME=$1
NUMBER_OF_NODES=$(($(yq '. | length' $FILE_NAME) -1 ))
echo "Running Test ${FILE_NAME}"
yq $FILE_NAME
echo "Starting ${NUMBER_OF_NODES} nodes"

cargo build
for i in $(seq 0 $NUMBER_OF_NODES); do
  cargo run -- $FILE_NAME $i &
done

wait