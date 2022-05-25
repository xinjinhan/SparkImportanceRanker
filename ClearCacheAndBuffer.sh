#!/bin/bash

# read allNodeList
n=0
unset slaves
while read line; do
  slaves[$n]=$line
  ((n++))
done < "slaves"

#execute command one by one
for server in ${slaves[*]}
do
    ssh $server sudo sysctl -w vm.drop_caches=3
done
