// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { logInfo, Contract } from "./ledger/api";
import { SimpleTemplate } from "./templates";

export function main(): void {
  logInfo("hello-world");

  let contract: Contract<SimpleTemplate> = new SimpleTemplate(
    "alice",
    42,
  ).create();

  logInfo(`created contract with ID ${contract.contractId()}`);
}
