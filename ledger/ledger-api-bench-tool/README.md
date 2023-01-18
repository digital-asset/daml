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

Benchtool's .yaml configuration file have two main sections: `submission` and `streams`.

### Submission config

#### Observer, divulgee, extra-submitter parties

You can specify the number of observer, divulgee and extra-submitter parties to create as follows:
```yaml
submission:
  ...
  num_observers: <NUMBER-OF-OBSERVERS>
  num_divulgees: <NUMBER-OF-DIVULGEES>
  num_extra_submitters: <NUMBER-OF-EXTRA-SUBMITTERS>
```
This tells the benchtool to created the specified number of each kind of parties.  

The names of observers parties will follow the pattern of `Obs-<INDEX>` where `0 <= INDEX < NUMBER-OF-OBSERVERS`
and each party will have its own unique probability of `1 / 10^INDEX` for being selected as an observer of a contract.
For example, when creating four observer parties they will be named `Obs-0`, `Obs-1`, `Obs-2`, `Obs-3`
and their respective probabilities will be 100%, 10%, 1% and 0.1%.

Divulgee and extra-submitter parties behave analogously to the observer parties: their names will be respectively `Div-<INDEX>` 
and `Sub-<INDEX>` and their probabilities will be computed in the same way.

#### Contracts submission

You can control the details of contract instances to submit by specifying: 
- the contract templates to use, e.g. 'Foo1' and 'Foo2',
- the number of contracts to create for a given template by specifying its weight, e.g. for each 10 contracts 'Foo1' 
  create one contract 'Foo2',
- they size in bytes of the special payload argument to the contracts (which is realized as a random string of the given size).

There are three templates you can choose from: 'Foo1', 'Foo2' and 'Foo3'
and they all have identical implementations (except for their identifiers).

```yaml
submission:
  ...  
  num_instances: 100
  instance_distribution:
    - template: Foo1
      weight: 10
      payload_size_bytes: 1000
    - template: Foo2
      weight: 1
      payload_size_bytes: 1000
```

#### Consuming and nonconsuming exercises

You can specify consuming and nonconsuming exercises to submit as below.
```yaml
submission:
  ...
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

#### Observer party sets

You can specify a large number of observer parties using observer party sets.
```yaml
submission:
  ...
  observers_party_sets:
    - party_name_prefix: Bar
      count: 101
      visibility: 0.01
    - party_name_prefix: Baz
      count: 12
      visibility: 0.25
```
The above configuration snippet declares two party sets identified by their party name prefixes 'Bar' and 'Baz'. 
The 'Bar' party set will result in creating 101 parties: {Bar-001, Bar-002, .., Bar-100}. 
The 'Baz' party set will result in creating 12 parties: {Baz-01, Baz-02, .., Baz-11}.  
Notice how the index of each party is left padded with zeroes to make the lexicographic party name order and 
the numeric index order coincide. This is very helpful when you want to specify a subset of the parties from a party set 
in a filter specification of a streaming config.  
Each party from 'Bar' has 1% probability to become an observer on each new contract and each party from 'Baz' 
has 25% probability.


### Streams config

#### Bounding stream runtime or size

You can specify when to finish streaming the elements either on timeout or element count criterion as follows:
```yaml
streams:
  timeout: 10s
  max_item_count: 100
```

#### Party prefix filters

You can match multiple parties for a stream with party prefix filter.
```yaml
streams:
  ...
  party_prefix_filters:
    - party_name_prefix: Bar-02
    - party_name_prefix: Baz
```
Assuming the party sets from [Observer party sets](#observer-party-sets) sections, the above snippet is equivalent
to this one:
```yaml
streams:
  ...
  filters:
    - party: Bar-020
    - party: Bar-021
    - party: Bar-022
    - party: Bar-023
    - party: Bar-024
    - party: Bar-025
    - party: Bar-026
    - party: Bar-027
    - party: Bar-028
    - party: Bar-029
    - party: Baz-01
    - party: Baz-02
    - party: Baz-03
    - party: Baz-04
    - party: Baz-05
    - party: Baz-06
    - party: Baz-07
    - party: Baz-08
    - party: Baz-09
    - party: Baz-10
    - party: Baz-11
```
Here we used the prefixes of parties from party sets, but you are free to use arbitrary party 
prefixes and the will matched against all the existing parties.

#### Objectives

You can specify the objective that the stream should finish within the given time, for example in 1000 milliseconds:
```yaml
streams:
  ...
  objectives:
    max_stream_duration: 1000ms
```

You can specify the objective of the minimum and maximum rate of elements in the stream, for example at least 4000 
elements per second and at most 8000 elements per second.
```yaml
streams:
  ...
  objectives:
    min_item_rate: 4000
    max_item_rate: 8000
```

#### Ledger offset value

You can use any valid ledger offset.
Additionally, you can use two special values `"ledger-begin"` and `"ledger-end"`

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