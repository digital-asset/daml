// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.suites.NamesSpec._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class NamesSpec extends AnyWordSpec with Matchers {
  "test suite names" should {
    "only contain letters" in {
      all(allTestSuiteNames) should fullyMatch regex """[A-Za-z]+""".r
    }

    "not be a prefix of any other name, so that each suite can be included independently" in {
      allTestSuiteNames.foreach { name =>
        all(allTestSuiteNames.toSet - name) should not startWith name
      }
    }
  }

  "test identifiers" should {
    "only contain letters and numbers, and start with a letter" in {
      all(allTestIdentifiers) should fullyMatch regex """[A-Za-z][A-Za-z0-9]*""".r
    }

    "not be a prefix of or equal to any other name, so that each test can be included independently" in {
      allTestIdentifiers.zipWithIndex.foreach { case (testIdentifier, i) =>
        all(allTestIdentifiers.drop(i + 1)) should not startWith testIdentifier
      }
    }
  }

  "full test names" should {
    "be unique" in {
      allTestNames.foreach { name =>
        allTestNames.filter(_ == name) should have size 1
      }
    }
  }
}

object NamesSpec {
  private val allTestSuites =
    v1_dev.default(timeoutScaleFactor = 1) ++ v1_dev.optional(tlsConfig = None)
  private val allTestSuiteNames = allTestSuites.map(_.name).sorted

  private val allTests = allTestSuites.flatMap(_.tests)
  private val allTestIdentifiers = allTests.map(_.shortIdentifier)
  private val allTestNames = allTests.map(_.name).sorted
}
