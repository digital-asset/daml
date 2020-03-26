#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -euo pipefail

AUTH="Authorization: token $GITHUB_TOKEN"
MONTH=$1
START="$MONTH-01T00:00:00+00:00"
END="$(date -Is -u -d "$START + 1 month")"
FULL="$(${FULL+false})"
FILE="${2:-std-change-report-daml-$MONTH.csv}"
ERROR_FILE="${FILE}.err"

SHA_LIST=$(git log --format=%H --after $START --before $END | tac)

echo "Found $(echo $SHA_LIST | wc -w) commits. First: $(echo $SHA_LIST | sed 's/ /\n/g' | head -1), last: $(echo $SHA_LIST | sed 's/ /\n/g' | tail -1)."

TMP=$(mktemp -d)

echo -n "Getting data for each commit..."

mkdir -p "$TMP/commits"

curl -H "$AUTH" \
     -H "Accept: application/vnd.github.groot-preview+json" \
     --output "$TMP/commits/#1" \
     -s \
     "https://api.github.com/repos/digital-asset/daml/commits/{$(echo $SHA_LIST | sed 's/ /,/g')}/pulls"

echo " done."

PR_LIST=$(for sha in $SHA_LIST; do cat $TMP/commits/$sha | jq -r '.[] | .number'; done)

echo -n "Getting PR data for each commit..."
 mkdir -p "$TMP/pulls"

curl -H "$AUTH" \
     --output "$TMP/pulls/#1" \
     -s \
     "https://api.github.com/repos/digital-asset/daml/pulls/{$(echo $PR_LIST | sed 's/ /,/g')}"

echo " done."

echo -n "Getting issue data for each PR..."

mkdir -p "$TMP/issues"

curl -H "$AUTH" \
     --output "$TMP/issues/#1" \
     -s \
     "https://api.github.com/repos/digital-asset/daml/issues/{$(echo $PR_LIST | sed 's/ /,/g')}"

echo " done."

mkdir -p "$TMP/approvers"

echo -n "Getting approvers data..."

curl -H "$AUTH" \
     --output "$TMP/approvers/#1" \
     -s \
     "https://api.github.com/repos/digital-asset/daml/pulls/{$(echo $PR_LIST | sed 's/ /,/g')}/reviews"

echo " done."

echo "Writing export to $FILE..."

echo "\"date merged\",\"date opened\",\"title\",\"author\",\"approvers\",\"merger\",\"change type\",\"sha\",\"commit link\",\"PR\",\"PR link\"" > "$FILE"
for sha in $SHA_LIST; do
    COMMIT_LINK="https://github.com/digital-asset/daml/commit/$sha"
    PR=$(cat $TMP/commits/$sha | jq -r '.[] | .number')

    if [ -z "$PR" ]; then
        echo "$COMMIT_LINK" >> "$ERROR_FILE"
    else
        PR_URL=$(cat $TMP/commits/$sha | jq -r '.[] | .url')
        OPEN_DATE=$(cat $TMP/pulls/$PR | jq ".created_at")
        MERGE_DATE=$(cat $TMP/pulls/$PR | jq ".merged_at")
        AUTHOR=$(cat $TMP/pulls/$PR | jq '.user.login')
        MERGER=$(cat $TMP/pulls/$PR | jq '.merged_by.login')
        PR_LINK="https://github.com/digital-asset/daml/pull/$PR"
        TITLE=$(cat $TMP/pulls/$PR | jq '.title')
        CHANGE_TYPE=$(cat $TMP/issues/$PR | jq 'if [.labels[] | .name] | contains(["Standard-Change"]) then "Standard-Change" else "Routine-Change" end')
        APPROVERS=$(cat $TMP/approvers/$PR | jq '[.[] | select(.state=="APPROVED") | .user.login] | join(";")')
        if [ "$CHANGE_TYPE" = '"Standard-Change"' ] || [ "$FULL" = "true" ]; then
            echo "$MERGE_DATE,$OPEN_DATE,$TITLE,$AUTHOR,$APPROVERS,$MERGER,$CHANGE_TYPE,\"$sha\",\"$COMMIT_LINK\",\"$PR\",\"$PR_LINK\"" >> "$FILE"
        fi
    fi
done

if [ -f "$ERROR_FILE" ]; then
    echo "One or more errors occurred. Please manually check the following commits:"
    cat "$ERROR_FILE"
fi
