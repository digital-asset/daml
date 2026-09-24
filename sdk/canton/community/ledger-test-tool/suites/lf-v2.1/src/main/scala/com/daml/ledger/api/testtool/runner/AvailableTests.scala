// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.runner

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite

trait AvailableTests {
  def defaultTests: Vector[LedgerTestSuite]

  def optionalTests: Vector[LedgerTestSuite]
}
