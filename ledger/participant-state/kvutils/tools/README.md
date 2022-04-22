# `submission-entries-extractor`

This submission-entries-extractor tool extracts from a ledger export a set of so-called 
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

* `<ledger export file>`: is the full path of the ledger export file

* `<submission entries file>`: is the full path of the file in which the 
  resulting submission entries must be written.
