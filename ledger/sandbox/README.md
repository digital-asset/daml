# Overview
This document is to help internal engineers work with the Sandbox and the new ledger API.
Note: If you encounter bugs. Please report issues you find in the #team-ledger-api channel.

# DAML Sandbox

To build a fat JAR with the sandbox built from HEAD run

    bazel build //ledger/sandbox:sandbox-binary

Sandbox application can be run from command line with the following command:

    java -jar bazel-bin/ledger/sandbox/sandbox-binary_deploy.jar [options] <archive>

as run from the main project root directory (adjust the location of the JAR according to your working directory).

## Command line arguments

```
  -p, --port <value>       Sandbox service port. Defaults to 6865.
  -a, --address <value>    Sandbox service host. Defaults to binding on all addresses.
  --dalf                   Parse provided archives as DAML-LF Archives instead of DARs.
  --static-time            Use static time, configured with TimeService through gRPC.
  -w, --wall-clock-time    Use wall clock time (UTC). When not provided, static time is used.
  -o, --sim-time-offset <value>
                           Use simulated time with the given duration (ISO-8601 with optional `-` prefix) as offset relative to UTC. For example, supplying `-PT6M` will result in the server time lagging behind UTC by 6 minutes. When not provided, static time is used.
  --no-parity              Disables Ledger Server parity mode. Features which are not supported by the Platform become available.
  --scenario <value>       If set, the sandbox will execute the given scenario on startup and store all the contracts created by it.
  --daml-lf-archive-recursion-limit <value>
                           Set the recursion limit when decoding DAML-LF archives (.dalf files). Default is 1000
  <archive>...             Daml archives to load. Only DAML-LF v1 Archives are currently supported.
  --pem <value>            TLS: The pem file to be used as the private key
  --crt <value>            TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set.
  --cacrt <value>          TLS: The crt file to be used as the the trusted root CA.
  --help                   Print the usage text
```

# Compatibility

Sandbox uses models compiled in to the DAR format.

The dar files are the archives containing compiled DAML code. We highly recommend generating the dar files using the new DAML packaging, as described in https://engineering.da-int.net/docs/da-all-docs/packages/daml-project/. This will ensure that you're generating the .dar files correctly. The linked page also gives a good overview of what the dar files are, along with other key concepts.

Note that the new Ledger API only supports DAML 1.0 or above codebases compiled to DAML-LF v1. Again, using the DAML packaging as suggested above will ensure that you are generating dar files that the Sandbox can consume.

# Ledger API

The new Ledger API uses gRPC. You can find the full documentation of all the services involved rendered at http://ci.da-int.net/job/ledger-api/job/build/job/master/lastSuccessfulBuild/artifact/ledger-api/grpc-definitions/target/docs/index.html (save the file locally to get the styling to work). If you just want to create / exercise contracts, I suggest you start by looking at `command_service.proto`, which exposes a synchronous API to the DAML ledger. 
