// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.runner

import com.daml.ledger.api.testtool.infrastructure.{LedgerTestCase, LedgerTestSuite}

final class ConfiguredTests(availableTests: AvailableTests, config: Config) {
  val defaultTests: Vector[LedgerTestSuite] = availableTests.defaultTests
  val optionalTests: Vector[LedgerTestSuite] = availableTests.optionalTests

  val allTests: Vector[LedgerTestSuite] = defaultTests ++ optionalTests
  val missingTests: Set[String] = {
    val allTestCaseNames = allTests.flatMap(_.tests).map(_.name).toSet
    (config.included ++ config.excluded ++ config.additional).filterNot(prefix =>
      allTestCaseNames.exists(_.startsWith(prefix))
    )
  }

  val defaultCases: Vector[LedgerTestCase] = defaultTests.flatMap(_.tests)
  val optionalCases: Vector[LedgerTestCase] = optionalTests.flatMap(_.tests)
  val allCases: Vector[LedgerTestCase] = defaultCases ++ optionalCases
}
