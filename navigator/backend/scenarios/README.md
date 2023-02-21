### Navigator Testing Scenarios

The scenario(s) contained in this directory are useful to quickly spin up the sandbox and navigator from `HEAD` and visually inspect the navigator UI or perform other tests.

As an example, assuming your current working directory is the project root, you can:

1. open a terminal
1. change your working directory to `navigator/backend/scenarios/rental`
1. build the project

       daml build

1. start the sandbox and run a scenario

       bazel run --run_under="cd $PWD && " \
       //ledger/sandbox-classic:sandbox-classic-binary -- --scenario Main:example dist/rental.dar

1. open another terminal
1. change your working directory to `navigator/backend/scenarios/rental`
1. start the navigator

       bazel run --run_under="cd $PWD && " \
       //navigator/backend:navigator-binary -- server

1. open a browser
1. go to `localhost:4000`
1. play around

#### Usage with TLS

If you want to use a secure channel (e.g.: to test against an authenticated sandbox), you can use the test certificates in ``//test-common/test-certificates``.

To use them, run the sandbox as follows:

       bazel build //test-common/test-certificates
       bazel run --run_under="cd $PWD && " \
       //ledger/sandbox-classic:sandbox-classic-binary -- --scenario Main:example dist/rental.dar
       --pem $PWD/bazel-bin/test-common/test-certificates/server.pem --crt $PWD/bazel-bin/test-common/test-certificates/server.crt --cacrt $PWD/bazel-bin/test-common/test-certificates/ca.crt

And run navigator as follows:

       bazel run --run_under="cd $PWD && " \
       //navigator/backend:navigator-binary -- server \
       --pem $PWD/bazel-bin/test-common/test-certificates/client.pem --crt $PWD/bazel-bin/test-common/test-certificates/client.crt --cacrt $PWD/bazel-bin/test-common/test-certificates/ca.crt
