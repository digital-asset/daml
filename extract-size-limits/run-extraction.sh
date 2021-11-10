#!/bin/bash

# set -e

DBNAME='connect_1_0_1'
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
echo "Storing the extraction results in '$TAR'"
tar -czf "$TAR" csv/ sql/