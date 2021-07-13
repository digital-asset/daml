# Running tests against an Oracle database

Certain integration tests use Oracle as a DBMS for certain components (e.g. the JSON API).

These tests are executed separately on CI and you can see an up-to-date list of them in the [CI configuration](./ci/build.yml).

In order to execute them locally, the repository has a couple of conveniences:
  - `dev-env` automatically sets up the environment variables
    - `ORACLE_USERNAME` to `system`
    - `ORACLE_PWD` to `hunter2`
    - `ORACLE_PORT` to `1521`
  - `bazel.rc` includes the `oracle` configuration key that leverages the environment variables mentioned above

This means that _once you have an Oracle database running locally with the proper credentials and using the proper port_, you can
run tests that require Oracle as in the following example:

    bazel test --config=oracle //ledger-service/http-json-oracle/...

## Running an Oracle database locally

_If_ you have the necessary authorization, you can use the same process followed on CI to run Oracle.

Find the Oracle test job on the [CI configuration](./ci/build.yml) and run the commands that pull and run the Oracle database Docker image.

