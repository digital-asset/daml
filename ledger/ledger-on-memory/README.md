# Overview

This document is to help internal engineers work with `ledger-on-memory`.

`ledger-on-memory` is a key/value-based ledger that uses a simple in-memory map as an
underlying storage. It uses either H2 or PostgreSQL as its index database.

# Ledger On Memory

To build a fat JAR with the server built from HEAD run

    bazel build //ledger/ledger-on-memory:app_deploy.jar

The application can be run from command line with the following command:

    java -Xmx4G -XX:+UseG1GC -jar bazel-bin/ledger/ledger-on-memory/app_deploy.jar --participant participant-id=foo,port=6861

As run from the main project root directory (adjust the location of the JAR according to 
your working directory).

Alternatively, the application can be run using the Bazel command:

    bazel run //ledger/ledger-on-memory:app -- --participant participant-id=foo,port=6861

## Creating ledger exports

Ledger On Memory can be used to generate ledger exports through an environment variable:

    export KVUTILS_LEDGER_EXPORT=/path/to/export/file

Then launch the ledger using the Bazel or Java command as described above.
