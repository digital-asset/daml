// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.language.LanguageVersion
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TransactionVersionSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  "TransactionVersion.assignNodeVersion" should {

    "be stable" in {
      val testCases = Table(
        "language version" -> "transaction version",
        LanguageVersion.v1_6 -> TransactionVersion.V10,
        LanguageVersion.v1_7 -> TransactionVersion.V10,
        LanguageVersion.v1_8 -> TransactionVersion.V10,
        LanguageVersion.v1_11 -> TransactionVersion.V11,
        LanguageVersion.v1_12 -> TransactionVersion.V12,
        LanguageVersion.v1_13 -> TransactionVersion.V13,
        LanguageVersion.v1_14 -> TransactionVersion.V14,
        LanguageVersion.v1_15 -> TransactionVersion.V15,
        LanguageVersion.v1_dev -> TransactionVersion.VDev,
      )

      forEvery(testCases) { (languageVersion, transactionVersions) =>
        TransactionVersion.assignNodeVersion(languageVersion) shouldBe transactionVersions
      }
    }

    "be total" in {
      LanguageVersion.All.foreach(TransactionVersion.assignNodeVersion(_))
    }

    "surjective" in {
      LanguageVersion.All
        .map(TransactionVersion.assignNodeVersion(_))
        .toSet shouldBe TransactionVersion.All.toSet
    }

  }
}
