# `replay-benchmark`

This benchmarks the LF engine using transactions from a ledger entries stored in a file.

## Build

Build the tool with Bazel:

    bazel build //daml-lf/snapshot:replay-benchmark

## Running the tool

Run the tool using Bazel and pass the benchmark parameters using `-p`
jmh command line functionality:

     bazel run //daml-lf/snapshot:replay-benchmark --   \
      -p entriesFile=<entries files>                    \
      -p choiceName=<exercise choice names>             \
      [-p choiceIndex=<index of the exercise node>]     \
      [-p darFile=<dar files>]                                              


where:

* `<entries files>`: is the full path of the ledger entries
  files to be used separated by commas (`,`). At the time of writing,
  entries files can be created by the `submission-entries-extractor`
  residing in a private repository.

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


# `replay-profile`

This profiles the replay of a transaction built from a ledger entries file.

     bazel run //daml-lf/snapshot:replay-profile --     \
      --entries <entries files>                         \
      [--dar <dar files>]                               \
      --profile-dir <profile directory>                 \
      --choice <exercise choice names>                  \
      [--exercise-index <exercise index>]


where:

* `<entries files>`: is the full path of the ledger entries
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

* `<profile directory>` is the directory where the profiling output
  will be written.

