// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

declare module "*.png";
declare module "recharts";

declare module "babel-standalone" {
  export * from "babel-core";
}

declare module "@babel/standalone" {
  export * from "babel-core";
}

// Webpack loader to load the content of a file as a string
declare module "!raw-loader!*" {}

declare module "modernizr" {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  var _a: any;
  export = _a;
}

// Constants injected by Webpack
declare const __BUILD_VERSION__: string;
declare const __BUILD_COMMIT__: string;
