# How to Generate and Analyse Daml Choice Performance Data

These scripts allow a Daml multi-project to be profiled.

Transaction data is saved when daml-script function are ran. Alongside the saved transaction data, all Daml choice names
are also saved.

For each compiled Dar file and daml-script function, all called choices are JMH benchmarked with benchmark results being
saved in a JSON formatted file.

Once JMH benchmarking data has been collected, benchmark results are graphed as a multi-row bar chart follows:
- one row exists for each Dar file and script function name that has been analysed
- y-axis (for each row) is the JMH benchmark timing profile (typically in ms units)
- x-axis is the fully qualified choice name (without package ID)
- bar charts are coloured based on whether we are looking at baseline data or updated data
    - differences in the size of bars allows the speedup or slow down between 2 measurement sets to be compared
    - sometime, with larger data sets, it may be useful to zoom in on a set of choice data

TODO: add an example benchmarking graph

## Configuring a Daml Project for Snapshot and JMH Benchmarking



TODO: add an example JSON config file

## Generate Snapshot and Choice Name Files

To generate and save transaction snapshot and choice name data, run the following shell script:
```shell
./daml-lf/snapshot/scripts/generate-snapshots.sh $SNAPSHOT_CONFIG $DAML_PROJECT $DATA_DIR
```

## Run JMH Benchmarking Using Snapshot and Choice Name Data

To generate and save JMH benchmarking data, by replaying transaction snapshot files, run the following shell script:
```shell
./daml-lf/snapshot/scripts/generate-benchmarks.sh $SNAPSHOT_CONFIG $DAML_PROJECT $DATA_DIR
```

## Display Choice JMH Benchmarking Results

Having generated JMH benchmarking data for 2 versions of a project (e.g. where one project provides baseline data and
a revision introduces some Daml choice implementation changes), then the benchmarking results can be graphed and compared
using:
```shell
python3 ./daml-lf/snapshot/scripts/display-benchmarks.py $BASE_DATA_DIR $UPDATED_DATA_DIR
```

Running this Python script will open and display a Plotly bar graph in a browser window.
