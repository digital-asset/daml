# 1. Gatling Scenarios

## 1.1. Prerequisites
All current Gatling scenarios require `quickstart-model.dar` with IOU example. You can build one using:
```
bazel build //docs:quickstart-model
ls "${PWD}/bazel-bin/docs/quickstart-model.dar"
```

## 1.2. List of Scenarios
Gatling scenarios extend from `io.gatling.core.scenario.Simulation`:
- `com.daml.http.perf.scenario.CreateCommand`
- `com.daml.http.perf.scenario.ExerciseCommand`
- `com.daml.http.perf.scenario.CreateAndExerciseCommand`
- `com.daml.http.perf.scenario.AsyncQueryConstantAcs`
- `com.daml.http.perf.scenario.SyncQueryConstantAcs`
- `com.daml.http.perf.scenario.SyncQueryNewAcs`
- `com.daml.http.perf.scenario.SyncQueryVariableAcs`
- `com.daml.http.perf.scenario.OracleMultiUserQueryScenario`

# 2. Running Gatling Scenarios from Bazel

## 2.1. Help
```
$ bazel run //ledger-service/http-json-perf:http-json-perf-binary -- --help
```

## 2.2. Example
```
$ bazel run //ledger-service/http-json-perf:http-json-perf-binary-ce -- \
--scenario=com.daml.http.perf.scenario.CreateCommand \
--dars="${PWD}/bazel-bin/docs/quickstart-model.dar" \
--reports-dir=/home/leos/tmp/results/
```

## 2.3 Running MultiUserQueryScenario

For oracle *Query Store* since we use an external docker oracle vm, we might
want to retain the data between runs to specifically focus on testing query performance.
To achieve this use `RETAIN_DATA` and `USE_DEFAULT_USER` env vars to use a static 
user(`ORACLE_USER`) and preserve data.

This scenario uses a single template `KeyedIou` defined in `LargeAcs.daml`

We can control a few scenario parameters i.e `NUM_RECORDS` `NUM_QUERIES` `NUM_READERS` `NUM_WRITERS` via env variables

`RUN_MODE` allows you to run specific test case scenarios in isolation

1. Populate Cache

```

USE_DEFAULT_USER=true RETAIN_DATA=true RUN_MODE="populateCache" bazel run //ledger-service/http-json-perf:http-json-perf-binary-ee -- --scenario=com.daml.http.perf.scenario.OracleMultiUserQueryScenario --dars=$PWD/bazel-bin/ledger-service/http-json-perf/LargeAcs.dar --query-store-index oracle

```

2. Fetch By Key

Query contracts by the defined key field.

```

USE_DEFAULT_USER=true RETAIN_DATA=true RUN_MODE="fetchByKey" NUM_QUERIES=100 bazel run //ledger-service/http-json-perf:http-json-perf-binary-ee -- --scenario=com.daml.http.perf.scenario.OracleMultiUserQueryScenario --dars=$PWD/bazel-bin/ledger-service/http-json-perf/LargeAcs.dar --query-store-index oracle

```

3. Fetch By Query

Query contracts by a field on the payload which is the `currency` in this case.

```

USE_DEFAULT_USER=true RETAIN_DATA=true RUN_MODE="fetchByQuery" bazel run //ledger-service/http-json-perf:http-json-perf-binary-ee -- --scenario=com.daml.http.perf.scenario.OracleMultiUserQueryScenario --dars=$PWD/bazel-bin/ledger-service/http-json-perf/LargeAcs.dar --query-store-index oracle

```

# 3. Running Gatling Scenarios Manually

The following instructions tested on Linux but should also work on macOs.

## 3.1. Install Gatling (open-source load testing solution)
- the website: https://gatling.io/open-source
- direct URL: https://repo1.maven.org/maven2/io/gatling/highcharts/gatling-charts-highcharts-bundle/3.3.1/gatling-charts-highcharts-bundle-3.3.1-bundle.zip

## 3.2. Create quickstart Daml project
```
$ daml new quickstart-java --template quickstart-java
$ cd quickstart-java/
$ daml build
```

## 3.3. Start Sandbox with quickstart DAR
Ledger ID `MyLedger` is important, it is currently hardcoded in the `com.daml.http.perf.scenario.SimulationConfig`. See `aliceJwt`.
```
$ daml sandbox --ledgerid MyLedger --dar ./.daml/dist/quickstart-0.0.1.dar
```

## 3.4. Start JSON API
```
daml json-api  --ledger-host=localhost --ledger-port=6865 --http-port=7575 --package-reload-interval 5h --allow-insecure-tokens
```

## 3.5. Run Gatling scenario
```
$ <GATLING_HOME>/bin/gatling.sh --simulations-folder=<DAML_PROJECT_HOME>/ledger-service/http-json-perf/src/main/scala/com/daml/http/perf/scenario --simulation=com.daml.http.perf.scenario.CreateCommand
```
Where:
- `<GATLING_HOME>` -- path to the Gatling directory
- `<DAML_PROJECT_HOME>` -- path to the Daml Repository on the local disk
- `--simulation=com.daml.http.perf.scenario.CreateCommand` -- full class name of the scenario from the `--simulations-folder`
