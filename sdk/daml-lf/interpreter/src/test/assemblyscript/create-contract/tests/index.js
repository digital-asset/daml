// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import assert from "assert";
import { add } from "../build/debug.js";
assert.strictEqual(add(1, 2), 3);
console.log("ok");

