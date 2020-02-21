// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const moduleNameMapper = {
  '^@daml/types$': '../daml-types',
  '^@daml/ledger$': '../daml-ledger',
  // $1 used for @daml/react/ledgerStore
  '^@daml/react(.*)$': '../daml-react$1'
};

// Bazel workaround for:
//
//   Invalid hook call. Hooks can only be called inside of the body of a function component.
//
// By default the test-case loads `react/cjs/react.development.js` and
// fails with the above error. A similar issue with `ts_devserver` suggests
// that `rules_nodejs` bundles conflicting versions of react in some of the
// artifacts. See https://github.com/bazelbuild/rules_nodejs/issues/1383 .
//
// We do not want to use this workaround when invoking the test with
// `yarn test`. We us the existence of the environment variable
// `TEST_WORKSPACE` as a proxy for whether the test was invoked from bazel.
if (process.env.TEST_WORKSPACE !== undefined) {
  moduleNameMapper['^react$'] = '<rootDir>/../../../node_modules/react/umd/react.development.js';
}

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
