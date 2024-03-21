Reference Configurations
========================

This directory contains a set of reference configurations. The configurations aim to provide a
starting point for your own setup. The following configurations are included:

* `sandbox`: A simple setup for a single participant node connected to a single
  domain node, using in-memory stores for testing.
* `participant`: A participant node storing data within a PostgresSQL database.
* `domain`: A reference configuration for an embedded domain node which runs all the three domain node types in one process.

For the Enterprise Edition, the following configurations are included:

* `sequencer`: A high-throughput sequencer node.
* `mediator`: A mediator node.
* `manager`: A domain manager node configuration.

If you use TLS, note that you need to have an appropriate set of TLS certificates to run the example configurations.
You can use the `tls/gen-test-certs.sh` script to generate a set of self-signed certificates for testing purposes.

Please check the [installation guide](https://docs.daml.com/canton/usermanual/installation.html) for further details on how to run these configurations.
