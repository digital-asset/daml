# `sample-code-generation` project

**IMPORTANT**: Although this project is in a sub-directory of `ledger-client` it
is not part of the project defined in `ledger-client/build.sbt`.

_It is a standalone project_

This project contains a sample Daml library (in `src/main/daml/Main.daml`)
and the Scala sources generated from that library.

## Regenerating the sources

To regenerate the sources just run:

    $ ./regen-sources.sh

The generated sources will be placed in `src/main/scala/generated-sources`

