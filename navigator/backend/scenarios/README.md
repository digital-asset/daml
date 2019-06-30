### Navigator Testing Scenarios

The scenario(s) contained in this directory are useful to quickly spin up the sandbox and navigator from `HEAD` and visually inspect the navigator UI or perform other tests.

As an example, assuming your current working directory is the project root, you can:

1. open a terminal
1. change your working directory to `navigator/backend/scenarios/rental`
1. build the project

       daml build

1. start the sandbox and run a scenario

       bazel run --run_under="cd $PWD && " \
       //ledger/sandbox:sandbox-binary -- --scenario Main:example dist/rental.dar

1. open another terminal
1. change your working directory to `navigator/backend/scenarios/rental`
1. start the navigator

       bazel run --run_under="cd $PWD && " \
       //navigator/backend:navigator-binary -- server

1. open a browser
1. go to `localhost:4000`
1. play around