# OSX Dev Setup

In the base directory where you want your AssemblyScript projects to exist, run:
```shell
npm init
npm install --save-dev assemblyscript
npm install --save as-proto
npm install --save-dev as-proto-gen
```

# Creating an AssemblyScript package

In the directory where you want the AssemblyScript package to be created, enter:
```shell
npx asinit .
```

Then edit the generated `assembly/index.ts` file as required.

# Compiling an AssemblyScript Package to a WASM target

Note:
- by convention, we include a source language indicator when copying WASM files.

## `hello-world` Project

In the AssemblyScript package directory `./hello-world` run:
```shell
mkdir -p protobuf
protoc --plugin=protoc-gen-as=../node_modules/.bin/as-proto-gen --as_opt=gen-helper-methods --as_out=protobuf --proto_path ../../../../../transaction/src/main/protobuf/com/digitalasset/daml/lf value.proto
npm run asbuild
cp ./build/release.wasm ../../resources/hello-world.as.wasm
```

## `create-contract` Project

In the AssemblyScript package directory `./create-contract` run:
```shell
mkdir -p protobuf
protoc --plugin=protoc-gen-as=../node_modules/.bin/as-proto-gen --as_opt=gen-helper-methods --as_out=protobuf --proto_path ../../../../../transaction/src/main/protobuf/com/digitalasset/daml/lf value.proto
npm run asbuild
cp ./build/release.wasm ../../resources/create-contract.as.wasm
```

# Issues

AssemblyScript was unable to compile the LF `value.proto` file - there were issues with variants that required some manual patching.

All exported functions must be present in the entry point or `index.ts` file. This is, at least partly, related to 
AssemblyScript's use of tree shaking during compilation.

AssemblyScript classes may only have at most one constructor defined.

Attributes such as `private` and `protected` are not enforced.

AssemblyScript has no exception handling - though it is possible to throw exceptions!

AssemblyScript has no concept of interface or abstract class - so it is necessary to simulate this by using default 
implementations that throw unimplemented exceptions.

AssemblyScript does not allow generic class arguments to be constrained. All such constraints need to be made at runtime
(e.g. by checking value types) and appropriate exceptions thrown.

AssemblyScript does not allow the definition of inner classes and it does not allow anonymous classes to be defined.

AssemblyScript does not support function closures.

AssemblyScript is still under active development.
