// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.tls.TlsConfiguration

package object v1_dev {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    v1_15.default(timeoutScaleFactor)

  def optional(tlsConfig: Option[TlsConfiguration]): Vector[LedgerTestSuite] =
    v1_15.optional(tlsConfig)
}
