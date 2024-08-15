#!/bin/bash
RULE_ARGS=""
while IFS= read -r line || [[ -n "$line" ]]; do
    [[ $line != CORE-* ]] && line="CORE-$line"
    RULE_ARGS="$RULE_ARGS -r \"$line\""
done < /app/tests/testrulelist.txt
python /app/core.py validate -s "sdtmig" -v "3-3" -ct "sdtmct-2020-03-27" -d "/app/tests/test_suite" $RULE_ARGS
