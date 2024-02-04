#!/usr/bin/env bash

ENDPOINT=https://hooks.slack.com/services/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

curl -X POST -H 'Content-type: application/json' \
  --data '{"username": "'"$1"'", "text":"'"$2"'"}' \
  ${ENDPOINT}
