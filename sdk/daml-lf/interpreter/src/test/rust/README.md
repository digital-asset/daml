# OSX Dev Setup

```shell
brew install rustup libiconv protobuf
# NOTE: `cargo install wasm-pack` failed to build from source in M1 environments?
curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | 
export LIBRARY_PATH=$LIBRARY_PATH:$(brew --prefix)/lib:$(brew --prefix)/opt/libiconv/lib
export PATH="$HOME/.cargo/bin:$PATH"
cargo install protobuf-codegen
rustup target add wasm32-unknown-unknown 
```

# Creating a Rust package

In the directory where you want the Rust package to be created, enter:
```shell
cargo new --lib hello-world
```

Then edit the generated `src/lib.rs` file as required.

Reading Notes:
- https://www.hellorust.com/setup/wasm-target/

# Compiling a Rust Package to a WASM target

Note:
- by convention, we include a source language indicator when copying WASM files.

## `hello-world` Project

In the Rust package directory `./hello-world` run:
```shell
cargo build --target wasm32-unknown-unknown --release
cp ./target/wasm32-unknown-unknown/release/hello_world.wasm ../../resources/hello-world.rs.wasm
```

## `create-contract` Project

In the Rust package directory `./create-contract` run:
```shell
mkdir -p ./src/protobubf
protoc --rust_out ./src/protobubf --proto_path ../../../../../transaction/src/main/protobuf/com/digitalasset/daml/lf value.proto
cargo build --target wasm32-unknown-unknown --release
cp ./target/wasm32-unknown-unknown/release/create_contract.wasm ../../resources/create-contract.rs.wasm
```

## `fetch-contract` Project

In the Rust package directory `./fetch-contract` run:
```shell
mkdir -p ./src/protobubf
protoc --rust_out ./src/protobubf --proto_path ../../../../../transaction/src/main/protobuf/com/digitalasset/daml/lf value.proto
cargo build --target wasm32-unknown-unknown --release
cp ./target/wasm32-unknown-unknown/release/fetch_contract.wasm ../../resources/fetch-contract.rs.wasm
```

## `exercise-choice` Project

In the Rust package directory `./exercise-choice` run:
```shell
mkdir -p ./src/protobubf
protoc --rust_out ./src/protobubf --proto_path ../../../../../transaction/src/main/protobuf/com/digitalasset/daml/lf value.proto
cargo build --target wasm32-unknown-unknown --release
cp ./target/wasm32-unknown-unknown/release/exercise_choice.wasm ../../resources/exercise-choice.rs.wasm
```

# `data-interoperability` Project

In the Rust package directory `./data-interoperability` run:
```shell
mkdir -p ./src/protobubf
protoc --rust_out ./src/protobubf --proto_path ../../../../../transaction/src/main/protobuf/com/digitalasset/daml/lf value.proto
cargo build --target wasm32-unknown-unknown --release
cp ./target/wasm32-unknown-unknown/release/data_interoperability.wasm ../../resources/data-interoperability.rs.wasm
```

In the Daml package directory `./data-interoperability` run:
```shell
daml build
cp ./.daml/dist/data-interoperability-0.1.0.dar ../../resources/data-interoperability.dar
```
