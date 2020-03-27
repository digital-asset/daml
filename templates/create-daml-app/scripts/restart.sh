#!/bin/bash
while [ 1 ] ; do
  $1 &
  PID=$!
  fswatch --one-event --event Updated $2
  kill $PID
  sleep 1 # give it a window to receive SIGINT
done
