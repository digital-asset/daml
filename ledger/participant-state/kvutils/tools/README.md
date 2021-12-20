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
      -p choiceName=<exercise choice names>                                \
      [-p exerciseIndex=<index of the exercise node>]                      \
      [-p darFile=<dar files>]                                              


where:

* `<ledger export files>`: is the full path of the ledger export
  files to be used separated by commas (`,`)

* `<exercise choice names>`: is the full qualified choice name of the
  root exercise node to be benchmarked separated by commas (`,`). A full
  qualified choice name should be of the form
  `ModuleName:TemplateName:ChoiceName`.  Note the package ID is
  omitted. By default, the tool benchmarks the first choice with 
  such a name it finds in the ledger export.

* the optional parameter `<position of the exercise node>` is the 
  index of the exercise among the root exercise nodes that matches
  choice name specified by the `choiceName` parameter in the order 
  they appear in the export.

* the optional parameter `<dar files>` specify the full path of 
  the dar files to be used  separated by commas (`,`). If defined 
  the program contained in the dar file is used instead of one
  present in the ledger export, and the export is "adapted" to this 
  program. The adaptation process attempts to map the identifiers
  from the export file with the ones of dar file when those latter
  differ only in their package ID.  This can be used when the original
  Daml source used to generate the ledger export is only slightly
  modified or compiled with different options.
  
The tool expects the exercised choices to be unique in the ledger
export.  If two or more transactions in the export exercise the same
choice (same template name same choice name), both are ignored.
