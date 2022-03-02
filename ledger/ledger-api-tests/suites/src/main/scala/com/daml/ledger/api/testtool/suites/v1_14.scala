// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite

package object v1_14 {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    (v1_8.default(timeoutScaleFactor) ++ Vector(
      new ExceptionRaceConditionIT,
      new ExceptionsIT,
    )).sortBy(_.name)

  def optional(): Vector[LedgerTestSuite] =
    v1_8.optional()
}
