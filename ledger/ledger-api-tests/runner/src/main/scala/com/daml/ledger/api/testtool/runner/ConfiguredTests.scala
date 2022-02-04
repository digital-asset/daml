// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.runner

import com.daml.ledger.api.testtool.infrastructure.{LedgerTestCase, LedgerTestSuite}
import com.daml.ledger.api.testtool.performance.PerformanceTests

final class ConfiguredTests(availableTests: AvailableTests, config: Config) {
  val defaultTests: Vector[LedgerTestSuite] = availableTests.defaultTests
  val optionalTests: Vector[LedgerTestSuite] = availableTests.optionalTests
  val performanceTests: PerformanceTests = availableTests.performanceTests

  val allTests: Vector[LedgerTestSuite] = defaultTests ++ optionalTests
  val missingTests: Set[String] = {
    val allTestCaseNames = allTests.flatMap(_.tests).map(_.name).toSet
    (config.included ++ config.excluded).filterNot(prefix =>
      allTestCaseNames.exists(_.startsWith(prefix))
    )
  }

  val missingPerformanceTests: Set[String] =
    config.performanceTests.filterNot(performanceTests.names(_))

  val defaultCases: Vector[LedgerTestCase] = defaultTests.flatMap(_.tests)
  val optionalCases: Vector[LedgerTestCase] = optionalTests.flatMap(_.tests)
  val allCases: Vector[LedgerTestCase] = defaultCases ++ optionalCases
}
