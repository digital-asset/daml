#!/usr/bin/env bash
set -eu
hsTmpDir=$1

ghc                                         \
    --make                                  \
    -odir $hsTmpDir                         \
    -hidir $hsTmpDir                        \
    -o $hsTmpDir/simpleDecodeDotProto       \
    $hsTmpDir/TestProto.hs                  \
    $hsTmpDir/TestProtoImport.hs            \
    $hsTmpDir/TestProtoOneof.hs             \
    $hsTmpDir/TestProtoOneofImport.hs       \
    tests/SimpleDecodeDotProto.hs           \
    >/dev/null
