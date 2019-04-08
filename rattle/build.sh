#!/bin/bash
cd "$(dirname "$0")/.."
stack exec --package=shake --package=filepattern --stack-yaml=rattle/stack.yaml -- runhaskell -irattle rattle/Main.hs
