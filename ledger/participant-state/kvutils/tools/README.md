# `integrity-check`

A tool that checks the integrity of a ledger dump stored in a file. It can be used to
verify that a dump made with an old version of the software can still be successfully processed.
It can also be used to profile Daml interpretation code in a more isolated fashion.

## Build

Build the tools with Bazel:

    bazel build //ledger/participant-state/kvutils/tools:all

## Preparing a dump

You can produce a ledger dump using the in memory kv ledger implementation contained in the
`ledger-on-memory` project. For details please consult its README file.

## Running the tool

Run the tool using Bazel:

    bazel run //ledger/participant-state/kvutils/tools:integrity-check <ledger dump file>

The following options are supported:

    --help
    --skip-byte-comparison      Skips the byte-for-byte comparison. Useful when comparing behavior across versions.
    --sort-write-set            Sorts the computed write set. Older exports sorted before writing. Newer versions order them intentionally.
    --index-only                Run only the indexing step of the integrity checker (useful to benchmark the indexer).
    --jdbc-url                  External JDBC url (useful for running against PostgreSQL).
    --report-metrics            Print all registered metrics.
    --expected-updates-output   The output path for expected updates. Useful for debugging. It might be worth sorting the output files and using a diff tool.
    --actual-updates-output     Similarly, the output path for actual updates.

# `submission-entries-extractor`

This submission-entries-extractor tool extract from ledger export a set of so-called 
"submission entries" which are readable by LF. Do not delete/change without sync with 
the language team.

## Build 

Build the tool with Bazel:

    bazel build //ledger/participant-state/kvutils/tools:submission-entries-extractor
    
## Running the tool 

Run the tool using Bazel and pass the benchmark parameters using `-p`
jmh command line functionality:

    bazel run //ledger/participant-state/kvutils/tools:submission-entries-extractor -- \
      <ledger export file>                                                             \
      -o <submission entries file>

where:

* `<ledger export files>`: is the full path of the ledger export file

* `<submission entries file>`: is the full path of the file in which the 
  resulting submission entries must be written.