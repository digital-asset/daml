// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export type Either<L, R> =
  | { type: "left"; value: L }
  | { type: "right"; value: R };

export function right<L, R>(value: R): Either<L, R> {
  return { type: "right", value };
}

export function left<L, R>(value: L): Either<L, R> {
  return { type: "left", value };
}

export function fmap<L, R, R2>(
  a: Either<L, R>,
  fn: (a: R) => Either<L, R2>,
): Either<L, R2> {
  switch (a.type) {
    case "left":
      return a;
    case "right":
      return fn(a.value);
  }
}
