#!/usr/bin/env bash

ENDPOINT=https://slack.com/api/files.upload
TOKEN=xoxp-203166533634-203215597956-226930941700-53aaf6b9d6b70904080a9a0c16cce795

curl -F file=@"$1" \
  -F channels=dev-ci \
  -F token=${TOKEN} \
  ${ENDPOINT}
