#!/usr/bin/env bash
if [ "$#" -eq 1 ]; then
    target_machine=$1
    echo "Connecting to "$target_machine
else
    echo "Usage: ssh-machine [target_machine]"
    exit 1
fi



rsync  perfuchs@$target_machine:/scratch/per/experimentRunner/results/* brickResults
