#!/usr/bin/env bash

for filename in test-input/*.csv; do
   echo "Processing $filename"	
   awk 'BEGIN{FS=";"}NR>1{printf "%s;%s;%s\n", $2,$1,$3}' $filename > /tmp/input
  ./test.sh /tmp/input /tmp/out 2>/tmp/err
  name=`basename $filename`
  paste -d';' /tmp/input /tmp/out > test-output/$name
done
