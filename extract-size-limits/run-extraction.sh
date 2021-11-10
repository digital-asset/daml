#!/bin/bash

# set -e

DBNAME='chess_db'
EXTRACT="sudo -u postgres psql -D$DBNAME --csv"


for SQL in $(ls *sql)
do
    
    CSV="$(basename -s .csv $SQL)"
    echo "Extracting $SQL -> $CSV"
   
    $EXTRACT > $CSV
done
