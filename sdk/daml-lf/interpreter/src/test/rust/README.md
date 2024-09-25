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

## `data-interoperability` Project

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

# Working with and Compiling a Python Project to a WASM Target

The following example code assumes that the developer has access to the `py2wasm` docker image that will host our 
development environment.

## `surface-language` Project

In the Python project directory `./surface-language` run:
```shell
pip install -r requirements.txt
mkdir -p ./src/protobuf
protoc --python_out ./src/protobuf --proto_path ../../../../../transaction/src/main/protobuf/com/digitalasset/daml/lf value.proto
docker run --rm -it --platform linux/amd64 -v .:/home/py2wasm/workdir py2wasm
cd workdir
mkdir -p build
py2wasm surface-language.py -o ./build/surface-language.py.wasm # FIXME:
exit
cp ./build/surface-language.py.wasm ../../resources/surface-language.py.wasm
```

Instead of running `py2wasm` (within the docker container shell), one can do the following instead:
```shell
INPUT=surface-language.py
export CC=~/.local/lib/python3.11/site-packages/nuitka/wasi-sdk/21/sdk-Linux/bin/clang
python -m nuitka $INPUT --standalone --static-libpython=yes --disable-ccache --lto=yes --output-dir=/home/py2wasm/.local/lib/python3.11/site-packages/nuitka/__py2wasm --output-filename=output.wasm --generate-c-only
```

This latter mode of compiling Python code to a WASM binary has the advantage that the generated Nuitka C source files (located in `/home/py2wasm/.local/lib/python3.11/site-packages/nuitka/__py2wasm/$(basename $INPUT .py).build/`) 
may be modified and then recompiled using:
```shell
python -m nuitka $INPUT --standalone --static-libpython=yes --disable-ccache --lto=yes --output-dir=/home/py2wasm/.local/lib/python3.11/site-packages/nuitka/__py2wasm --output-filename=output.wasm --recompile-c-only
cp /home/py2wasm/.local/lib/python3.11/site-packages/nuitka/__py2wasm/$(basename $INPUT .py).dist/output.wasm ../../resources/$(basename $INPUT).wasm
```

Notes:
- import C function stubs need to be non-static and annotated with `__attribute__((import_module("env"), import_name("my_import_function_stub")))` - if import symbols are not used, they will be eliminated by the WASM compiler
- export C function definitions need to be non-static and annotated with `__attribute__((export_name("my_export_function_definition")))`
