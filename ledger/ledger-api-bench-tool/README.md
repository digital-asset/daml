# ledger-api-bench-tool

The `ledger-api-bench-tool` is a tool for measuring performance of a ledger.
It allows to run multiple concurrent streams reading transactions from a ledger and provides performance statistics
for such streams.

Please note that the `ledger-api-bench-tool` does not provide a load source for the ledger.

## Running
Run using `bazel run`:
```
bazel run -- //ledger/ledger-api-bench-tool --help
```
or using a fat jar:
```
bazel build //ledger/ledger-api-bench-tool:ledger-api-bench-tool_deploy.jar
java -jar bazel-bin/ledger/ledger-api-bench-tool/ledger-api-bench-tool_deploy.jar --help
```

## Metrics

### `CountRateMetric`
Number of elements processed per second.

Unit: `[elem/s]`

```periodic value = (number of elements in the last period) / (period duration)```

```final value = (total number of elements processed) / (total duration)```

### `TotalCountMetric`
Total number of processed elements.

Unit: `[-]`

```
periodic value = (number of elements processed so far)
final value = (total number of elements processed)
```

### `SizeMetric`
Amount of data processed per second.

Unit: `[MB/s]`

```
periodic value = (number of megabytes processed in the last period) / (period duration)

final value = (total number of megabytes processed) / (total duration)
```

### `DelayMetric`
Record time delay of a stream element is defined as follows:

```
record time delay = (current time) - (record time of a processed element)
```

The delay metric measures mean delay of elements processed in a period of time.

Unit: `[s]`

```
periodic value = (mean record time delay of elements processed in a period of time)

final value = N/A
```

Note that in case of running the `ledger-api-bench-tool` against a ledger with live data, the delay metric
is expected to converge to `0s` which is equivalent to being up-to-date with the most recent data.

### `ConsumptionSpeedMetric`
Describe the ratio between a time span covered by record times of processed elements to the period duration.

Unit: `[-]`

Additional definitions:

```
previous latest record time = (record time of the latest element from periods before the current period)

latest record time = (record time of the latest element in the current period OR undefined in case of a period without a single element)
```

```
periodic value =
  if (latest record time != undefined) ((latest record time) - (previous latest record time)) / (period duration)
  else 0.0
  
final value = N/A
```