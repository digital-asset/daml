// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/* eslint-disable @typescript-eslint/no-non-null-assertion */
import Ledger from "@daml/ledger";
import { ContractId, Party, Template, Choice, ChoiceFrom } from "@daml/types";

// Regression test for #8338, we only care that this compiles.

const ledger = new Ledger({ token: "" });

type X = { p: Party };

type Archive = {};

// Note that the codegen will generate only X with the type intersected with Y.
// However, due to what looks like a typescript bug that does not trigger the
// error in TS 3.8. It does however, trigger the error in TS >= 3.9.
// To make sure we hit this, we separate them here.
const X: Template<X, undefined, "pkg-id:M:X"> = undefined!;

const Y: {
  Archive: Choice<X, Archive, {}, undefined> &
    ChoiceFrom<Template<X, undefined>>;
} = undefined!;

const cid: ContractId<X> = undefined!;

export const f = async () => {
  await ledger.exercise(Y.Archive, cid, {});
  await ledger.createAndExercise(Y.Archive, { p: "Alice" }, {});
};
