// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.configuration

/** @param optionalLedgerId The ID of the target ledger. If defined, the client will only
  *                         communicate with ledgers that have the expected LedgerId.
  *                         Note that this setting only affects the binding process, when the ledger
  *                         ID on the server is checked.
  */
final case class LedgerIdRequirement(optionalLedgerId: Option[String]) {

  def enabled: Boolean = optionalLedgerId.isDefined

  def isAccepted(checkedLedgerId: String): Boolean =
    optionalLedgerId.fold(true)(checkedLedgerId.equals)

  def copy(ledgerId: Option[String]): LedgerIdRequirement =
    LedgerIdRequirement(optionalLedgerId = ledgerId)

  @deprecated("Use Option-based copy", "1.3.0")
  def copy(ledgerId: String): LedgerIdRequirement =
    LedgerIdRequirement(this.optionalLedgerId.map(_ => ledgerId))
}

object LedgerIdRequirement {

  val none: LedgerIdRequirement = LedgerIdRequirement(None)
  def matching(ledgerId: String): LedgerIdRequirement = LedgerIdRequirement(Some(ledgerId))

  @deprecated("Use Option-based constructor", "1.3.0")
  def apply(ledgerId: String, enabled: Boolean): LedgerIdRequirement =
    LedgerIdRequirement(if (enabled) Some(ledgerId) else None)
}
