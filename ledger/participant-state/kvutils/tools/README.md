# `integrity-check`

A tool that checks the integrity of a ledger dump stored in a file. It can be used to
verify that a dump made with an old version of the software can still be successfully processed.
It can also be used to profile DAML interpretation code in a more isolated fashion.

## Build

Build the tool with Bazel:

    bazel build //ledger/participant-state/kvutils/tools:integrity-check

## Preparing a dump

You can produce a ledger dump using the in memory kv ledger implementation contained in the
`ledger-on-memory` project. For details please consult its README file.

## Running the tool

Run the tool using Bazel:

    bazel run //ledger/participant-state/kvutils/tools:integrity-check <ledger dump file>
