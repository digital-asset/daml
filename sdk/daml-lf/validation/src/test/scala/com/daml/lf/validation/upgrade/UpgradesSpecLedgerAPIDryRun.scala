// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation
package upgrade

class UpgradesSpecLedgerAPIDryRun
    extends UpgradesSpecLedgerAPI("Ledger API with dry run")
    with LongTests {
  override val uploadSecondPackageDryRun = true;
}
