// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.common

import com.digitalasset.daml.lf.data.Ref

sealed abstract class LedgerIdMode extends Product with Serializable

object LedgerIdMode {

  /**
    * Ledger ID is provided by the test fixture and the Ledger API endpoint behind it is expected to use it.
    */
  final case class Static(ledgerId: Ref.LedgerName) extends LedgerIdMode

  /**
    * Ledger ID is selected by the Ledger API endpoint behind the fixture. E.g. it can be random in case of Sandbox, or pre-existing in case of remote Ledger API servers.
    */
  final case class Dynamic() extends LedgerIdMode
}
