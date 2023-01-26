// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.suites.v1_15.ExplicitDisclosureIT
import com.daml.ledger.api.tls.TlsConfiguration

package object v1_14 {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    (v1_8.default(timeoutScaleFactor) ++ Vector(
      new ExceptionRaceConditionIT,
      new ExceptionsIT,
      new LimitsIT,
      new ExplicitDisclosureIT,
    )).sortBy(_.name)

  def optional(tlsConfig: Option[TlsConfiguration]): Vector[LedgerTestSuite] =
    v1_8.optional(tlsConfig)
}
