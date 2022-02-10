// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite

package object v1_dev {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    v1_14.default(timeoutScaleFactor)

  def optional(): Vector[LedgerTestSuite] =
    v1_14.optional()
}
