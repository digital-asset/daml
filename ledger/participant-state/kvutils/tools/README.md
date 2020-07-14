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

# `benchmark-replay`

A tool base on JMH that benchmark LF engine using transactions from a
ledger export stored in a file.

## Build 

Build the tool with Bazel:

    bazel build  //ledger/participant-state/kvutils/tools:benchmark-replay 
    
    
## Running the tool 

Run the tool using Bazel and pass the benchmark parameters using `-p`
JHM command line functionality:

    bazel run //ledger/participant-state/kvutils/tools:replay -- \
      -p ledgerFile=<ledger export files>                        \
      -p darFile=<dar files>                                     \
      -p choiceName=<exercise choice names>                      \
      [-p adapt=true]                                            \



* `<ledger export files>` : is the full path of the ledger export
  files to be tested separate by commas (`,`)

* `<dar files>` : is the full path of the dar files to be tested
  separate by commas (`,`)

* `<exercise choice names`>: is the full qualified choice names of the
  exercises to be tests separate by commas (`,`).  a full qualified
  choice name should be of the form `ModuleName:TemplateName:ChoiceName`. 
  Note the package ID is omitted. 

* the optional parameter `adapt=true` can be set to enable dar-export 
  adaptation. The adaptation process attempt to map the identifiers from the export
  file with the one of dar file when those latter differ only in their package ID.
  This can be use when the original DAML source use to generate the ledger export 
  is only slightly modified or compiled with different option. 
  
The tool expects the exercised choice to be unique in the ledger
export.  If two or more transactions in the export exercise the same
choice (same template Name same choice Name), both are ignored.