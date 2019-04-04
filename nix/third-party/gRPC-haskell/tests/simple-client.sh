#!/usr/bin/env bash
set -eu
hsTmpDir=$1

ghc                            \
    --make                     \
    -threaded                  \
    -odir $hsTmpDir            \
    -hidir $hsTmpDir           \
    -o $hsTmpDir/simple-client \
    $hsTmpDir/Simple.hs        \
    tests/TestClient.hs        \
    > /dev/null
