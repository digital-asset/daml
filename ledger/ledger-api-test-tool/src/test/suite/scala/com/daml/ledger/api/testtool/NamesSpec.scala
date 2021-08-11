// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.NamesSpec._
import com.daml.ledger.api.testtool.tests.Tests
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class NamesSpec extends AnyWordSpec with Matchers {
  "test suite names" should {
    "only contain letters" in {
      all(allTestSuiteNames) should fullyMatch regex """[A-Za-z]+""".r
    }

    "not be a prefix of any other name, so that each suite can be included independently" in {
      allTestSuiteNames.foreach { name =>
        all(allTestSuiteNames - name) should not startWith name
      }
    }
  }

  "test identifiers" should {
    "only contain letters and numbers, and start with a letter" in {
      all(allTestIdentifiers) should fullyMatch regex """[A-Za-z][A-Za-z0-9]*""".r
    }

    "not be a prefix of any other name, so that each test can be included independently" in {
      allTestIdentifiers.foreach { testIdentifier =>
        all(allTestIdentifiers - testIdentifier) should not startWith testIdentifier
      }
    }
  }
}

object NamesSpec {
  private val allTestSuites = (Tests.default() ++ Tests.optional ++ Tests.retired).toSet
  private val allTestSuiteNames = allTestSuites.map(_.name)

  private val allTests = allTestSuites.flatMap(_.tests)
  private val allTestIdentifiers = allTests.map(_.shortIdentifier)
}
