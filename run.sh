#!/bin/bash

for f in *.bz2; do {
  echo "Process \"$f\" started";
  bzip2 -d $f & pid=$!
  PID_LIST+=" $pid";
} done

trap "kill $PID_LIST" SIGINT

echo "Parallel processes have started";

wait $PID_LIST

echo
echo "All processes have completed"