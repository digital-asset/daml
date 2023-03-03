// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.runner

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite

trait AvailableTests {
  def defaultTests: Vector[LedgerTestSuite]

  def optionalTests: Vector[LedgerTestSuite]
}
