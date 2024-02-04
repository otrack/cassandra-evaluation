#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
REBAR=rebar3
POST=${DIR}/text.sh

REPORT=$(${REBAR} cover -v)
COV=$(echo "${REPORT}" |
      grep total |
      grep -v vcd_total_order |
      awk '{print $4}')

echo "${REPORT}"

BAR_NUMBER=17
LEFT=$(echo "${COV} ${BAR_NUMBER}" | awk '{print int(($1*$2)/100)}')
RIGHT=$(echo "${LEFT} ${BAR_NUMBER}" | awk '{print $2-$1}')
LEFTBAR=$(yes █ | head -"${LEFT}" | tr -d "\n")
RIGHTBAR=$(yes ░ | head -"${RIGHT}" | tr -d "\n")
BAR="${LEFTBAR}${RIGHTBAR} ${COV}"

echo "${BAR}"

## show coverage bar on slack
${POST} "COVERAGE BOT" "${BAR}"
