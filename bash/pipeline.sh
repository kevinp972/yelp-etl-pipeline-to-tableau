#!/bin/bash

# Change if these commands are not on the path.
DUCKDB=duckdb
SPARK=spark-submit

# File paths based on your directory structure
TRANSFORMER=../spark/spark-job.py
DATABASE=../duckdb/yelp.db
OUTPUT=../spark/output
QUERIES=../duckdb/duckdb-job.sql

# Variables for DuckDB load.
LOADPATH="$OUTPUT/*.parquet"

# Ensuring atomic execution - rollback on failure
rollback() {
    rm -fr $OUTPUT
    rm -f $DATABASE
}

message() {
    printf "%50s\n" | tr " " "-"
    printf "$1\n"
    printf "%50s\n" | tr " " "-"
}

check() {
    if [ $? -eq 0 ]; then
        message "$1"
    else 
        message "$2"
        rollback
        exit 1
    fi
}

run_spark() {
    rm -fr $OUTPUT
    $SPARK \
        --master local[*] \
        --conf "spark.sql.shuffle.partitions=2" \
        --name "Yelp Data Processing" \
        $TRANSFORMER \
        /home/anshadui/team17/data/yelp_academic_dataset_business.json \
        /home/anshadui/team17/data/yelp_academic_dataset_checkin.json \
        /home/anshadui/team17/data/yelp_academic_dataset_tip.json \
        $OUTPUT
    check "Spark job successfully completed." "Spark job FAILED."
}

run_duckdb() {
    sed "s|\$LOADPATH|${LOADPATH//\//\\/}|g" "$QUERIES" | $DUCKDB "$DATABASE"
    check "Data loaded into DuckDB successfully." "Data load FAILED."
}

message "\n\nSTARTING YELP DATA PIPELINE...\n\n"

run_spark
run_duckdb

check "PROCESS COMPLETE" "PIPELINE FAILED"
