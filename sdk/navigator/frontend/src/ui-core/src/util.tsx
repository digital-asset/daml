// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as Moment from "moment";
import * as React from "react";
import { DamlLfDataType, DamlLfType } from "./api/DamlLfType";
import { DamlLfValue } from "./api/DamlLfValue";

/**
 * The argument doesnt match the specified type.
 */
export class TypeError extends Error {
  constructor(
    private parameter: Record<string, unknown>,
    private argument?: Record<string, unknown>,
  ) {
    super();
    console.error("Argument", argument, "does not match parameter", parameter);
  }
  toString(): string {
    return `Argument ${this.argument} does not match parameter ${this.parameter}`;
  }
  toJSON(): Record<string, unknown> {
    return {
      argument: this.argument,
      parameter: this.parameter,
      msg: this.toString(),
    };
  }
}

export interface TypeErrorElementProps {
  parameter: DamlLfType | DamlLfDataType;
  argument: DamlLfValue;
}

export const TypeErrorElement: React.FunctionComponent<
  TypeErrorElementProps
> = ({ argument, parameter }: TypeErrorElementProps) => {
  const message = `Argument ${JSON.stringify(
    argument,
  )} does not match parameter ${JSON.stringify(parameter)}`;
  return <em>{message}</em>;
};

/**
 * No match found for the given object, even though the compiler reported the match to be
 * exhaustive. This can happen when matching on non-validated input.
 *
 * Use as follows:
 * ```
 * function foo(param: 'true' | 'false'): boolean
 *   switch (param) {
 *     case 'true': return true;
 *     case 'false': return false;
 *   }
 *   // The next line should never be reached.
 *   // At compile time, the compiler will complain if it cannot
 *   // prove that the next line is unreachable (i.e., above match
 *   // is not exhaustive).
 *   // At run time, this will throw an exception if this line is
 *   // still reached (i.e., function called with wrong parameters).
 *   throw new NonExhaustiveMatch(param);
 * }
 * ```
 */
export class NonExhaustiveMatch extends Error {
  constructor(private match: never) {
    super();
    console.error("No match found for ", match);
  }
  toString(): string {
    return `No match found for ${this.match}`;
  }
  toJSON(): Record<string, unknown> {
    return {
      match: this.match,
    };
  }
}

// ISO 8601 string to Moment
export function utcStringToMoment(str: string): Moment.Moment | undefined {
  const result = Moment.utc(str);
  if (result.isValid()) {
    return result;
  } else {
    return undefined;
  }
}

// Moment to ISO 8601 string
export function momentToUtcString(m: Moment.Moment): string {
  return m.utc().format();
}
