#!/bin/bash -v
cd "$(dirname "$0")"
cp build.sbt ../daml-lf
cd ../daml-lf
(cd archive; protoc da/*.proto --java_out=src/main/scala)
(cd transaction/src/main/protobuf;  protoc com/digitalasset/daml/lf/*.proto --java_out=../scala)
sbt compile
#sbt test -- not all working yet. and transaction/test fails even to compile.
