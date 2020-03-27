#!/bin/bash

DAR=.daml/dist/create-daml-app-0.1.0.dar

daml build -o ${DAR}

if [ $? -eq 0 ]; then
  daml codegen ts ${DAR} -o daml-ts/src
  pushd daml-ts
  yarn build
  popd
  daml ledger upload-dar ${DAR}
fi
