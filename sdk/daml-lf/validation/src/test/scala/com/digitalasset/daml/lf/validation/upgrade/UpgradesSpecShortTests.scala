package com.daml.lf.validation.upgrade

class UpgradesSpecShortTests
  extends UpgradesSpecAdminAPI("Admin API without validation")
    with ShortTests {
  override val disableUpgradeValidation = true
}

class UpgradesSpecLedgerAPIWithoutValidation
  extends UpgradesSpecLedgerAPI("Ledger API without validation")
    with ShortTests {
  override val disableUpgradeValidation = true
}
