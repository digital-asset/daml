// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.digitalasset.canton.config.TlsClientConfig

object Tests {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    suites.v2_dev.default(timeoutScaleFactor)

  def optional(tlsConfig: Option[TlsClientConfig]): Vector[LedgerTestSuite] =
    suites.v2_dev.optional(tlsConfig)

  val lfVersion = "2.dev"
}
