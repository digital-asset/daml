// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.tls.TlsConfiguration

package object v2_dev {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    v2_1.default(timeoutScaleFactor)

  def optional(tlsConfig: Option[TlsConfiguration]): Vector[LedgerTestSuite] =
    v2_1.optional(tlsConfig)
}
