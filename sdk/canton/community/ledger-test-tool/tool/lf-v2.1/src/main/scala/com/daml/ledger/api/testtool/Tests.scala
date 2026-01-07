// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.digitalasset.canton.config.TlsClientConfig

object Tests {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    suites.v2_1.default(timeoutScaleFactor)

  def optional(tlsConfig: Option[TlsClientConfig]): Vector[LedgerTestSuite] =
    suites.v2_1.optional(tlsConfig)

  val lfVersion = "2.1"
}
