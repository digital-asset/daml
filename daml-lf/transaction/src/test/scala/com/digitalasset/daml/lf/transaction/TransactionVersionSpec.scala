// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import value.{ValueVersion, ValueVersions}
import com.daml.lf.language.LanguageVersion
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TransactionVersionSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  "TransactionVersions.assignNodeVersion" should {

    val Seq(v1_6, v1_7, v1_8) = Seq("6", "7", "8").map(minor =>
      LanguageVersion(LanguageVersion.Major.V1, LanguageVersion.Minor.Stable(minor)))

    val v1_dev =
      LanguageVersion(LanguageVersion.Major.V1, LanguageVersion.Minor.Dev)

    val testCases = Table(
      "language version" -> "transaction version",
      v1_6 -> TransactionVersion("10"),
      v1_7 -> TransactionVersion("10"),
      v1_8 -> TransactionVersion("10"),
      v1_dev -> TransactionVersion("dev"),
    )

    "be stable" in {
      forEvery(testCases) { (languageVersion, transactionVersions) =>
        TransactionVersions.assignNodeVersion(languageVersion) shouldBe transactionVersions
      }
    }

  }

  "TransactionVersions.assignValueVersion" should {
    "be stable" in {

      val testCases = Table(
        "input" -> "output",
        TransactionVersion("10") -> ValueVersion("6"),
        TransactionVersion("dev") -> ValueVersion("dev")
      )

      forEvery(testCases) { (input, expectedOutput) =>
        TransactionVersions.assignValueVersion(input) shouldBe expectedOutput
      }

    }
  }

  "ValueVersions.Empty" should {
    "be empty" in {
      ValueVersions.Empty.nonEmpty shouldBe false
    }
  }

  "TransactionVersions.Empty" should {
    "be empty" in {
      TransactionVersions.Empty.nonEmpty shouldBe false
    }
  }

}
