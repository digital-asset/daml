// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.common

import com.daml.lf.data.Ref
import com.daml.ledger.api.domain.LedgerId

sealed abstract class LedgerIdMode extends Product with Serializable {
  def or(other: => LedgerId): LedgerId
}

object LedgerIdMode {

  def static(ledgerId: String): LedgerIdMode =
    Static(LedgerId(Ref.LedgerString.assertFromString(ledgerId)))

  def dynamic: LedgerIdMode =
    Dynamic

  /** The ledger ID is provided by the user or test fixture,
    * and the Ledger API endpoint behind it is expected to use it.
    */
  final case class Static(ledgerId: LedgerId) extends LedgerIdMode {
    override def or(other: => LedgerId): LedgerId = ledgerId
  }

  /** The ledger ID is selected by the ledger.
    * Typically, it will be random if the ledger and the participant are unified, or pre-existing
    * if the ledger is separate. With this option, Sandbox will generate a new ledger ID on first
    * run.
    */
  case object Dynamic extends LedgerIdMode {
    override def or(other: => LedgerId): LedgerId = other
  }
}
