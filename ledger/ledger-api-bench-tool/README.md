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

## Configuration

### Observer Parties

You can specify the number of observer parties to create in the .yaml config as follows:
```yaml
submission:
  num_observers: <NUMBER-OF-OBSERVERS>
```
This will tell the BenchTool to create `NUMBER-OF-OBSERVERS` parties.  
The names of these parties will follow the pattern of `Obs-<INDEX>` where `0 <= INDEX < NUMBER-OF-OBSERVERS`
and each party will have its own unique probability of `1 / 10^INDEX` for being selected as an observer of a contract.

For example, when creating four observer parties they will be named `Obs-0`, `Obs-1`, `Obs-2`, `Obs-3`
and their respective probabilities will be 100%, 10%, 1% and 0.1%.

### Ledger Offset

You can use any valid ledger offset.
Additionally, you can use two special values `"ledger-begin"` and `"ledger-end"`

### Consuming and Nonconsuming Exercises

You can specify consuming and nonconsuming exercises to submit as below.
```yaml
submssion:
  nonconsuming_exercises:
    probability: 2.3
    payload_size_bytes: 1000
  consuming_exercises:
    probability: 0.4
    payload_size_bytes: 200
```
For nonconsuming exercises `probability` can be any positive number. 
For example probability of `2.3` means that, for each create contract command,
there will be at least two nonconsuming exercises submitted and 30% chance of a third one.

For consuming exercises `probablity` must in range `[0.0, 1.0]`.

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