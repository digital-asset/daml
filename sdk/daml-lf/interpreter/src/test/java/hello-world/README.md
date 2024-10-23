# OSX Dev Setup

As for the Daml repo

# Compiling a Java/Grade Project to a WASM target

Note:
- by convention, we include a source language indicator when copying WASM files.

In the Java project directory `./hello-world` run:
```shell
export VERSION=2024-05-10
wget https://repo.maven.apache.org/maven2/de/mirkosertic/bytecoder/bytecoder-cli/$VERSION/bytecoder-cli-$VERSION-executable.jar
./gradlew clean assemble
mkdir -p ./build/wasm/classes
unzip ./build/libs/hello-world-0.0.1-SNAPSHOT.jar -d ./build/wasm/classes
java -jar bytecoder-cli-$VERSION-executable.jar compile wasm -classpath=./build/wasm/classes -mainclass='user.UserMain' -builddirectory=./build/wasm
cp ./build/wasm/bytecoderwasmclasses.wasm ../../resources/hello-world.java.wasm
```

# Issues

WASM byte code needs to link in a runtime. Bytecoder achieves this for WASM-JS targets by building a JS file named `bytecoderruntime.js`
that needs to be linked in when the JS WASM target runtime is defined. An analagous file needs to be defined in order to
ensure that the `hello-world.java.wasm` target can be ran.

Looking at how one might perform memory allocations and deallocations, there was no documentation or obvious example
code. As a result, memory management with bytecoder remains an open issue.
