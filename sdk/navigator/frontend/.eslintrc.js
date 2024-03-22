// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

module.exports = {
    "env": {
        "browser": true
    },
    "extends": [
        "eslint:recommended",
        "plugin:react/recommended",
        "plugin:@typescript-eslint/recommended"
    ],
    "parser": "@typescript-eslint/parser",
    "parserOptions": {
        "project": "tsconfig.json",
    },
    "plugins": [
        "react",
        "@typescript-eslint",
    ],
    "rules": {
      "react/display-name": "off",
      // Triggers falsely on FCs if you don’t annotate the argument as well, see
      // https://github.com/yannickcr/eslint-plugin-react/issues/2353
      "react/prop-types": "off",
      "@typescript-eslint/ban-types": [
        "error",
        {
          "extendDefaults": true,
          "types": {
            // While {} is problematic in a lot of cases, using it as the
            // props types is fine and this is very common in React, see
            // https://github.com/typescript-eslint/typescript-eslint/issues/2063#issuecomment-675156492
            "{}": false,
          },
        },
      ],
      // Warning on redundant type annotations seems silly.
      "@typescript-eslint/no-inferrable-types": "off",
      "@typescript-eslint/explicit-module-boundary-types": [
        "error",
        {
          "allowArgumentsExplicitlyTypedAsAny": true,
        },
      ],
      "@typescript-eslint/no-unused-vars": [
        "warn",
        {
          "varsIgnorePattern": "^_",
          "argsIgnorePattern": "^_",
        }
      ],
    },
    "settings": {
      "react": {
        "version": "detect",
      },
    },
};

