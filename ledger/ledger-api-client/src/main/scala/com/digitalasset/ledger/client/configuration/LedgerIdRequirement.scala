// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.configuration

/**
  * @param ledgerId The ID of the target ledger. If defined, the client will only communicate with ledgers that have the
  *                 expected LedgerId. Note that this setting only affects the binding process, when the ledger ID
  *                 on the server is checked.
  */
final case class LedgerIdRequirement(ledgerId: Option[String]) {
  def isAccepted(checkedLedgerId: String): Boolean =
    ledgerId.fold(true)(checkedLedgerId.equals)

  def copy(ledgerId: Option[String]): LedgerIdRequirement =
    LedgerIdRequirement(ledgerId = ledgerId)

  @deprecated("Use Option-based copy", "1.3.0")
  def copy(ledgerId: String): LedgerIdRequirement =
    LedgerIdRequirement(this.ledgerId.map(_ => ledgerId))
}

object LedgerIdRequirement {

  val none: LedgerIdRequirement = LedgerIdRequirement(None)
  def matching(ledgerId: String): LedgerIdRequirement = LedgerIdRequirement(Some(ledgerId))

  @deprecated("Use Option-based constructor", "1.3.0")
  def apply(ledgerId: String, enabled: Boolean): LedgerIdRequirement =
    LedgerIdRequirement(if (enabled) Some(ledgerId) else None)
}
