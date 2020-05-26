// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.configuration

/**
  * @param ledgerId The ID of the target ledger.
  * @param enabled If true, the client will only communicate with ledgers that have the expected LedgerId.
  *                Note that this setting only affects the binding process, when the ledger ID on the server is checked.
  */
final case class LedgerIdRequirement(ledgerId: String, enabled: Boolean) {

  def isAccepted(checkedLedgerId: String): Boolean = !enabled || checkedLedgerId == ledgerId
}
