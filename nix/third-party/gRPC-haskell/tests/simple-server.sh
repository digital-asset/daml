#!/usr/bin/env bash
set -eu
hsTmpDir=$1

ghc                            \
    --make                     \
    -threaded                  \
    -odir $hsTmpDir            \
    -hidir $hsTmpDir           \
    -o $hsTmpDir/simple-server \
    $hsTmpDir/Simple.hs        \
    tests/TestServer.hs        \
    > /dev/null
