#!/usr/bin/env bash
set -eu

pyTmpDir=$1

python                          \
    -m grpc.tools.protoc        \
    -I tests                    \
    --python_out=$pyTmpDir      \
    --grpc_python_out=$pyTmpDir \
    tests/simple.proto
