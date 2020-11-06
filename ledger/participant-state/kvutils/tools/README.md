# `integrity-check`

A tool that checks the integrity of a ledger dump stored in a file. It can be used to
verify that a dump made with an old version of the software can still be successfully processed.
It can also be used to profile DAML interpretation code in a more isolated fashion.

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
    --skip-byte-comparison  Skips the byte-for-byte comparison. Useful when comparing behavior across versions.
    --sort-write-set        Sorts the computed write set. Older exports sorted before writing. Newer versions order them intentionally.
    --index-only            Run only the indexing step of the integrity checker (useful to benchmark the indexer).
    --jdbc-url              External JDBC url (useful for running against PostgreSQL).
    --report-metrics          Print all registered metrics.

# `benchmark-replay`

This benchmarks the LF engine using transactions from a ledger export stored in a file.

## Build 

Build the tool with Bazel:

    bazel build //ledger/participant-state/kvutils/tools:benchmark-replay 
    
## Running the tool 

Run the tool using Bazel and pass the benchmark parameters using `-p`
jmh command line functionality:

    bazel run //ledger/participant-state/kvutils/tools:benchmark-replay -- \
      -p ledgerFile=<ledger export files>                                  \
      -p darFile=<dar files>                                               \
      -p choiceName=<exercise choice names>                                \
      [-p adapt=true]

where:

* `<ledger export files>`: is the full path of the ledger export
  files to be used separated by commas (`,`)

* `<dar files>` : is the full path of the dar files to be used
  separated by commas (`,`)

* `<exercise choice names`>: is the full qualified choice name of the
  exercises to be benchmarked separated by commas (`,`).  A full
  qualified choice name should be of the form
  `ModuleName:TemplateName:ChoiceName`.  Note the package ID is
  omitted.

* the optional parameter `adapt=true` can be set to enable dar-export
  "adaptation". The adaptation process attempts to map the identifiers
  from the export file with the ones of dar file when those latter
  differ only in their package ID.  This can be used when the original
  DAML source used to generate the ledger export is only slightly
  modified or compiled with different options.
  
The tool expects the exercised choices to be unique in the ledger
export.  If two or more transactions in the export exercise the same
choice (same template name same choice name), both are ignored.
