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
