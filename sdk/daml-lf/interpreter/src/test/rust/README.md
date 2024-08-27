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

In the Rust package directory run:
```shell
mkdir -p ./src/lf
protoc --rust_out ./src/lf --proto_path ../../../../../transaction/src/main/protobuf/com/digitalasset/daml/lf value.proto
cargo build --target wasm32-unknown-unknown --release  
```

This generates a WASM target in the directory `./target/wasm32-unknown-unknown/release/hello_world.wasm`.

To use compiled WASM code within tests, we need to then copy that code into the `test/resources` directory with:
```shell
cp ./target/wasm32-unknown-unknown/release/hello_world.wasm ../../resources/hello-world.rs.wasm
```
in the case of the `create-contract` project, with:
```shell
cp ./target/wasm32-unknown-unknown/release/create_contract.wasm ../../resources/create-contract.rs.wasm
```
and, in the case of the `fetch-contract` project, with:
```shell
cp ./target/wasm32-unknown-unknown/release/fetch_contract.wasm ../../resources/fetch-contract.rs.wasm
```

Note:
- by convention, we include a source language indicator when copying WASM files.
