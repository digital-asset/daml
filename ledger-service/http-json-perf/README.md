# 1. Gatling Scenarios

## 1.1. Prerequisites

All current Gatling scenarios require the `quickstart-model.dar` with IOU example. You can build one using:
```
bazel build //docs:quickstart-model
ls "${PWD}/bazel-bin/docs/quickstart-model.dar"
```
The particular dar is generated from several .daml files but the most relevant is:
`docs/source/app-dev/bindings-java/quickstart/template-root/daml/Iou.daml`
for full information regarding the generation of the testing dar please see [BUILD.bazel](../../docs/BUILD.bazel) and
refer to the `quickstart-model` rule.

The MultiUserQueryScenario use the LargeAcs.dar target which is the build from the daml source file on the following path:
`ledger-service/http-json-perf/daml/LargeAcs.daml`, the build can be found here [BUILD.bazel](BUILD.bazel) over
`daml_compile` rule.

## 1.2. List of Scenarios

Gatling scenarios extend from `io.gatling.core.scenario.Simulation`:
- `com.daml.http.perf.scenario.CreateCommand`
- `com.daml.http.perf.scenario.ExerciseCommand`
- `com.daml.http.perf.scenario.CreateAndExerciseCommand`
- `com.daml.http.perf.scenario.AsyncQueryConstantAcs`
- `com.daml.http.perf.scenario.SyncQueryConstantAcs`
- `com.daml.http.perf.scenario.SyncQueryNewAcs`
- `com.daml.http.perf.scenario.SyncQueryVariableAcs`
- `com.daml.http.perf.scenario.MultiUserQueryScenario`

# 2. Running Gatling Scenarios from Bazel

The solution maintain 2 edition flavours which are configurable with the suffix `ce` and `ee`, the latter is the one that can be set to run oracle DB.

## 2.1. Help

There are several configurations that the scenarios accepts, please refer to the help entry as follows:
```
$ bazel run //ledger-service/http-json-perf:http-json-perf-binary-ce -- --help
      or
$ bazel run //ledger-service/http-json-perf:http-json-perf-binary-ee -- --help
```
## 2.2 query-storage-index
By default, the solution will use in memory DB for the query-storage, however, it can utilise oracle and postgres.

To use oracle, set the arguement `--query-store-index="oracle"`, keep in mind that the flavour has to be set to ee suffix.
When running oracle the project use a custom oracle docker image, version can be look over here [oracle_image](../../ci/oracle_image),
please log in into your docker account using your credentials and pull the image beforehand.

There are a couple of environment variables that need to be set in order to connect properly the solution to the DB,
more information can be found here [BAZEL-oracle.md](../../BAZEL-oracle.md). Particularly see `ORACLE_PWD=hunter2`.

To run this image you may use the following instruction:
- On Mac
```
$ docker run -d --rm --name oracle -p 1521:1521 -e ORACLE_PWD=$ORACLE_PWD $IMAGE
```
- On Linux
```
$ docker run -d --rm --name oracle --network host -e ORACLE_PWD=$ORACLE_PWD $IMAGE
```

To use postgres you can do so in any flavour (ce/ee), just set the parameter as --query-store-index="postgres"
and the solution will spin up a postgres vm on demand.

## 2.3.Example

```
$ bazel run //ledger-service/http-json-perf:http-json-perf-binary-ce -- \
--scenario=com.daml.http.perf.scenario.CreateCommand \
--dars="${PWD}/bazel-bin/docs/quickstart-model.dar" \
--query-store-index="postgres" \
--reports-dir=/tmp/daml/ \
--max-duration=10min
```
Note: the path for the `reports-dir` argument has to be created beforehand and this is where the reports will be
generated, if missing there will not be any report generated.

## 2.4 Running on Intellij IDE

To run the implementation directly from Intellij to get access to debug tools or just simple for convenience,
follow the steps to export the project into the IDE described [here](../../BAZEL.md).

Then you will get access to bazel run configurations option. Open the `run/debug configuration` window and add new
configuration as Bazel Command, on the new configuration at in the target section
`//ledger-service/http-json-perf:http-json-perf-binary-ce` or `//ledger-service/http-json-perf:http-json-perf-binary-ee`
depending on the flavour of your choice. For the `Bazel command`  select 'run' on the dropdown. In the `Executable flags`
add all the arguments you want to run e.g.
```
--scenario=com.daml.http.perf.scenario.CreateCommand
--dars="<Path to quickstart-model.dar>"
--reports-dir="<Path to the report generation>"
```

Then just apply the changes and use the IDE tools to run or debug the solution.

## 2.5 Running MultiUserQueryScenario

As mentioned before this is scenario is the only one that at the moment is based in different dar than the other scenarios,
please be aware of that.

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
