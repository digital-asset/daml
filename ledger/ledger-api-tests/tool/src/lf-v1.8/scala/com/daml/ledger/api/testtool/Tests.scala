// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.tls.TlsConfiguration

object Tests {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    suites.v1_8.default(timeoutScaleFactor)

  def optional(tlsConfig: Option[TlsConfiguration]): Vector[LedgerTestSuite] =
    suites.v1_8.optional(tlsConfig)
}
