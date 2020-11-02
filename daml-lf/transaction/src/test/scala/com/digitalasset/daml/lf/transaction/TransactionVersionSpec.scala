// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import value.{ValueVersion, ValueVersions}
import com.daml.lf.language.LanguageVersion
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

class TransactionVersionSpec extends WordSpec with Matchers with TableDrivenPropertyChecks {

  "TransactionVersions.assignVersions" should {

    import VersionTimeline.Implicits._

    val Seq(v1_1, v1_5, v1_6, v1_7) = Seq("1", "5", "6", "7").map(minor =>
      LanguageVersion(LanguageVersion.Major.V1, LanguageVersion.Minor.Stable(minor)))

    val v1_dev =
      LanguageVersion(LanguageVersion.Major.V1, LanguageVersion.Minor.Dev)

    val langVersions = Table("language version", v1_1, v1_5, v1_6, v1_7)

    "pick always the min supported version for package using LF 1.7 or earlier" in {
      val supportedVersionRanges =
        Table(
          "supported Versions",
          VersionRange(TransactionVersion("10"), TransactionVersion("10")),
          VersionRange(TransactionVersion("10"), TransactionVersion("dev")),
          VersionRange(TransactionVersion("dev"), TransactionVersion("dev")),
        )

      forEvery(supportedVersionRanges) { supportedTxVersions =>
        val expectedTxVersion = supportedTxVersions.min
        val expectedOutput = Right(expectedTxVersion)

        TransactionVersions.assignVersions(supportedTxVersions, Seq.empty) shouldBe expectedOutput
        forEvery(langVersions) { langVersion =>
          TransactionVersions.assignVersions(supportedTxVersions, Seq(langVersion)) shouldBe expectedOutput
        }
      }
    }

    "pick version 11 for package using LF 1.dev" in {
      val supportedVersionRanges =
        Table(
          "supported Versions",
          VersionRange(TransactionVersion("10"), TransactionVersion("dev")),
          VersionRange(TransactionVersion("dev"), TransactionVersion("dev")),
        )

      forEvery(supportedVersionRanges) { supportedTxVersions =>
        val expectedTxVersion = TransactionVersion("dev")
        val expectedOutput = Right(expectedTxVersion)

        TransactionVersions.assignVersions(supportedTxVersions, Seq(v1_dev)) shouldBe expectedOutput
        forEvery(langVersions) { langVersion =>
          TransactionVersions.assignVersions(supportedTxVersions, Seq(langVersion, v1_dev)) shouldBe expectedOutput
        }
      }
    }

    "fail if the inferred version is not supported" in {
      forEvery(langVersions) { langVersion =>
        TransactionVersions.assignVersions(
          VersionRange(TransactionVersion("9"), TransactionVersion("9")),
          Seq(langVersion)) shouldBe 'left
        TransactionVersions.assignVersions(
          VersionRange(TransactionVersion("10"), TransactionVersion("10")),
          Seq(v1_dev, langVersion)) shouldBe 'left
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
