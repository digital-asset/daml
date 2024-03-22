// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const moduleNameMapper = {
  '^@daml/types$': '../daml-types',
  '^@daml/ledger$': '../daml-ledger',
  // $1 used for @daml/react/ledgerStore
  '^@daml/react(.*)$': '../daml-react$1'
};

module.exports = {
  testEnvironment: "node",
  testMatch: [
    "**/__tests__/**/*.+(ts|tsx|js)",
    "**/?(*.)+(spec|test).+(ts|tsx|js)"
  ],
  transform: {
    "^.+\\.(ts|tsx)$": "ts-jest",
    "^.+\\.(js|jsx)$": "babel-jest"
  },
  moduleNameMapper,
}
