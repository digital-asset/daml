// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.infrastructure.{LedgerTestCase, LedgerTestSuite}
import com.daml.ledger.api.testtool.performance.{Envelope, PerformanceTests}

final class ConfiguredTests(config: Config) {
  val defaultTests: Vector[LedgerTestSuite] =
    Tests.default(timeoutScaleFactor = config.timeoutScaleFactor)
  val optionalTests: Vector[LedgerTestSuite] = Tests.optional()
  val allTests: Vector[LedgerTestSuite] = defaultTests ++ optionalTests
  val performanceTests: PerformanceTests =
    PerformanceTests.from(Envelope.All, outputPath = config.performanceTestsReport)

  val missingTests: Set[String] = {
    val allTestCaseNames = allTests.flatMap(_.tests).map(_.name).toSet
    (config.included ++ config.excluded).filterNot(prefix =>
      allTestCaseNames.exists(_.startsWith(prefix))
    )
  }

  val defaultCases: Vector[LedgerTestCase] = defaultTests.flatMap(_.tests)
  val optionalCases: Vector[LedgerTestCase] = optionalTests.flatMap(_.tests)
  val allCases: Vector[LedgerTestCase] = defaultCases ++ optionalCases
}
