#!/bin/bash -v
cd "$(dirname "$0")"
cp build.sbt ../daml-lf
cd ../daml-lf
# better if the generated stuff lived in a single "gen" subdir
rm -rf archive/src/main/scala/com/digitalasset/daml_lf/
rm -rf transaction/src/main/scala/com/digitalasset/daml/lf/blinding
rm -f transaction/src/main/scala/com/digitalasset/daml/lf/transaction/TransactionOuterClass.java
rm -f transaction/src/main/scala/com/digitalasset/daml/lf/value/ValueOuterClass.java
#sbt clean
