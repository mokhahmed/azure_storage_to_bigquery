#!/bin/bash

name=$1
source=$2
sink=$3
token=$4
notification_topic="projects/$5/topics/$6"

echo "{\"sasToken\": \"$token\"}" > creds.json

cat creds.json

echo "create a transfer job from azure $source to gcp $sink bucket...."

gcloud transfer jobs create $source $sink \
--name="${name}_job" \
--source-creds-file='creds.json' \
--overwrite-when='different' \
--include-modified-after-relative=1d \
--notification-pubsub-topic=$notification_topic \
--notification-event-types='failed','aborted','success' \
--notification-payload-format='json' \
--schedule-repeats-every=1d