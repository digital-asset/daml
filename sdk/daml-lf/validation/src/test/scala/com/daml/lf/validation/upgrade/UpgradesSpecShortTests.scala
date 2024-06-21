// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation
package upgrade

class UpgradesSpecAdminAPIWithoutValidation
    extends UpgradesSpecAdminAPI("Admin API without validation")
    with ShortTests {
  override val disableUpgradeValidation = true;
}

class UpgradesSpecLedgerAPIWithoutValidation
    extends UpgradesSpecLedgerAPI("Ledger API without validation")
    with ShortTests {
  override val disableUpgradeValidation = true;
}
