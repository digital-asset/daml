// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.runner.{AvailableTests, TestRunner}

object Main {
  def main(args: Array[String]): Unit = {
    val config = CliParser.parse(args).getOrElse(sys.exit(1))
    val availableTests = new AvailableTests {
      override def defaultTests: Vector[LedgerTestSuite] =
        Tests.default(timeoutScaleFactor = config.timeoutScaleFactor)

      override def optionalTests: Vector[LedgerTestSuite] =
        Tests.optional(config.tlsConfig)
    }
    new TestRunner(availableTests, config).runAndExit()
  }
}
