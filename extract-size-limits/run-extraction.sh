#!/bin/bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# One-off script to extract maximal sizes from events and packages stored in an IndexDB of a Daml participant node.
#
# Usage:
#   1. Adjust the DBNAME and PSQL invocations below to point to your DB and use your authentication method.
#   2. call the script using 'bash run-extraction.sh'
#   3. Inspect the resulting .csv files. The extraction queries are written for different IndexDB versions, which is
#      why not all .csv files will contain any content. At least one of the 'event_V<xxx>.csv' files should contain data.
#   4. Share the packages .tar.gz file with other stakeholders wrt understanding size limits of your workflow.


DBNAME='chess_db'
PSQL="sudo -u postgres psql -d$DBNAME"

echo "-- starting extraction --"

mkdir -p "csv/$DBNAME"

for SQL in $(ls sql/*.sql)
do
    
    CSV="csv/$DBNAME/$(basename -s .sql $SQL).csv"
    echo "Extracting $SQL -> $CSV"
   
    $PSQL --csv -f "$SQL" > "$CSV"
done

TAR="db-limits-extraction-$(date -u -I).tar.gz"

echo "-- extraction complete --"
echo "See 'csv/$DBNAME' for the resulting .csv files. "
echo "Packaging them into '$TAR' for easy sharing."
tar -czf "$TAR" csv/ sql/