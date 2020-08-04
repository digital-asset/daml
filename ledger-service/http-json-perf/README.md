# Instructions

## 1. Install Gatling (open-source load testing solution)
- the website: https://gatling.io/open-source
- direct URL: https://repo1.maven.org/maven2/io/gatling/highcharts/gatling-charts-highcharts-bundle/3.3.1/gatling-charts-highcharts-bundle-3.3.1-bundle.zip

## 2. Start Sandbox
Ledger ID `MyLedger` is important, it is currently hardcoded in the `com.daml.http.perf.scenario.SimulationConfig`. See `aliceJwt`.
```
$ daml sandbox --ledgerid MyLedger ./.daml/dist/quickstart-0.0.1.dar
```

## 3. Start JSON API
```
daml json-api  --ledger-host=localhost --ledger-port=6865 --http-port=7575 --package-reload-interval 5h --allow-insecure-tokens
```

## 4. Run Gatling scenario
```
$ <GATLING_HOME>/bin/gatling.sh --simulations-folder=<DAML_PROJECT_HOME>/ledger-service/http-json-perf/src/main/scala/com/daml/http/perf/scenario --simulation=com.daml.http.perf.scenario.CreateCommand
```
Where:
- `<GATLING_HOME>` -- path to the Gatling directory
- `<DAML_PROJECT_HOME>` -- path to the DAML Repository on the local disk
- `--simulation=com.daml.http.perf.scenario.CreateCommand` -- full class name of the scenario from the `--simulations-folder`

 