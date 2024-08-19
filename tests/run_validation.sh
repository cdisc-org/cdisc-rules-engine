#!/bin/bash

if [ -f /run/secrets/cdisc_api_key ]; then
    export CDISC_LIBRARY_API_KEY=$(cat /run/secrets/cdisc_api_key | tr -d "\r\n")
else
    echo "Error: cdisc_api_key secret is not available"
    exit 1
fi

echo "Starting cache update..."
python3.10 /app/core.py update-cache
echo "Cache update completed."

echo "Starting validation..."
# python3.10 /app/core.py validate -s "sdtmig" -v "3-3" -ct "sdtmct-2020-03-27" -d "/app/tests/test_suite" $(cat /app/tests/testrulelist.txt | sed "s/^/-r /")
RULE_ARGS=""
while IFS= read -r rule || [[ -n "$rule" ]]; do
    rule=$(echo "$rule" | tr -d '\r')
    RULE_ARGS+=" -r $rule"
done < /app/tests/testrulelist.txt

echo "python3.10 /app/core.py validate -s 'sdtmig' -v '3-3' -ct 'sdtmct-2020-03-27' -d '/app/tests/test_suite' $RULE_ARGS"
# Run the validation command with all rules
python3.10 /app/core.py validate -s "sdtmig" -v "3-3" -ct "sdtmct-2020-03-27" -d "/app/tests/test_suite" $RULE_ARGS
echo "Validation completed."

echo "Searching for the generated report..."
report_file=$(ls -t /app/CORE-Report-*.xlsx | head -n1)

if [ -n "$report_file" ]; then
    echo "Report found: $report_file"
    mkdir -p /app/output
    cp "$report_file" /app/output/
    echo "Report copied to /app/output/"
else
    echo "Error: Report file not found"
    exit 1
fi

exit 0