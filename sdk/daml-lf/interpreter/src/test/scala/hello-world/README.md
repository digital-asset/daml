# OSX Dev Setup

As for the Daml repo

# Compiling a Scala/Grade Project to a WASM target

Note:
- by convention, we include a source language indicator when copying WASM files.

In the Scala project directory `./hello-world` run:
```shell
mkdir -p ./src/main/java
protoc --java_out ./src/main/java  --proto_path ../../../../../transaction/src/main/protobuf/com/digitalasset/daml/lf value.proto
./gradlew clean build
cp ./build/generated/teavm/wasm/hello-world.wasm ../../resources/hello-world.scala.wasm
```

# Issues

TeaVM only implements the JDK classes defined at https://teavm.org/jcl-report. When the `generateWasm` Gradle task runs, 
it fails as certain JDK class members (e.g. `java.lang.Class.isHidden()`) are not implemented - and the Protobuf 
generators appear to need these JDK classes and members.

https://teavm.org/docs/runtime/java-classes.html describes (in the _Reflection_ section) how one may allow teaVM to have
reflective access to class fields and methods. It is not currently clear if implementing/extending the teaVM 
`ReflectionSupplier` would allow these Protobuf generators to complete successfully or not.

Modifying Protobuf and teaVM versions used did not help outcomes or provide any additional insights.

With the hello-world example, it was noted that the `logInfo` host function had 2 I32 parameters (where it should only 
have 1) in the generated WASM byte code - this caused linking exceptions (i.e. incompatible import type). Attempts to 
define struct based `ByteString` data types (by extending the teaVM `Structure` interface) did not alter outcomes.

Implementing the teaVM `teavm.logString` host function (e.g. using a Scala `println` style implementation) should allow
additional debugging of the hello-world example.

Possible workaround to Protobuf issues: implement our own serialization/deserialization solution for the guest/host memory interactions.
