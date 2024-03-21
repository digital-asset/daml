// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.tls.TlsConfiguration

package object v1_15 {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    v1_14.default(timeoutScaleFactor) ++ Vector(
      new InterfaceIT,
      new InterfaceSubscriptionsIT,
      new InterfaceSubscriptionsWithEventBlobsIT,
      new TransactionServiceFiltersIT,
      new ExplicitDisclosureIT,
      new EventQueryServiceIT,
    )

  def optional(tlsConfig: Option[TlsConfiguration]): Vector[LedgerTestSuite] =
    v1_14.optional(tlsConfig)
}
