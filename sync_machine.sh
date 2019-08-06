#!/bin/bash

cd "$(dirname "$0")"

if [ "$#" -eq 1 ]; then
    target_machine=$1
    echo "Connecting to "$target_machine
else
    echo "Usage: ssh-machine [target_machine]"
    exit 1
fi

user=perfuchs
server_dir=/scratch/per/experimentRunner

dirs="target/scala-2.11/*.jar"

for d in $dirs
do
        rsync -avh --exclude '.git*' $d $user@$target_machine:$server_dir
done
rsync -avh --exclude '.git*' src/best-computational-configuration.py $user@$target_machine:$server_dir







