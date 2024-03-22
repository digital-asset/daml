// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.generator

import cats.syntax.functorFilter._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite

import scala.reflect.ClassTag

object LedgerApiTestGeneratorSupport {

  def testEntries[TT: ClassTag, TS: ClassTag, TE](
      suites: List[LedgerTestSuite],
      testEntry: (String, String, TT, Boolean, Option[TS]) => TE,
  ): List[TE] = {
    suites.flatMap { suite =>
      val testSuite = suite match {
        case testSuite: TS => Some(testSuite)
        case _ => None
      }

      suite.tests
        .map { test => (test.suite.name, test.description) -> test.tags }
        .mapFilter { case ((testName, description), testTags) =>
          testTags.collectFirst { case testTag: TT =>
            testEntry(testName, description, testTag, false, testSuite)
          }
        }
    }
  }
}
