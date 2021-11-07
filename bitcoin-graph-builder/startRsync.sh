#!/usr/bin/env bash
# Simple script to sync source code a to a GCP VM.
# It's often faster to build the source there than to repeatedly copy the jar
# from local to VM.
user=$1
hostip=$2
while inotifywait -r ./**/*; do
    rsync -e "ssh -i ~/.ssh/google_compute_engine" -ravz --exclude "target/" . $user@$hostip:btc-source
done
