// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function catchToError(e: any): Error {
  if (e instanceof Error) {
    return e;
  } else {
    return new Error(`${e}`);
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function catchToString(e: any): string {
  if (e instanceof Error) {
    return e.message;
  } else {
    return `${e}`;
  }
}

// eslint-disable-next-line @typescript-eslint/ban-types
export function loadExportedFunction<F extends Function>(
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  exports: any,
  name: string,
  args: string[],
  defaultResult: F,
): F {
  if (!(name in exports)) {
    if (defaultResult) {
      return defaultResult;
    } else {
      throw new Error(`No function '${name}' exported.
      Use 'export function ${name}(${args.join(", ")}) {...}'.`);
    }
  }
  if (typeof exports[name] !== "function") {
    throw new Error(`Export '${name}' is not a function.
    Use 'export function ${name}(${args.join(", ")}) {...}'.`);
  }
  if (exports[name].length !== args.length) {
    throw new Error(`Export '${name}' is not a function with ${
      args.length
    } arguments.
    Use 'export function ${name}(${args.join(", ")}) {...}'.`);
  }

  return exports[name] as F;
}
