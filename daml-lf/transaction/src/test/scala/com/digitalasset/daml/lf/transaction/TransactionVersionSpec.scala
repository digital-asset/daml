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
        LanguageVersion.v1_14 -> TransactionVersion.V14,
        LanguageVersion.v1_15 -> TransactionVersion.V15,
        LanguageVersion.v1_dev -> TransactionVersion.VDev,
        // TODO(#17366): Map to TransactionVersion 2.1 once it exists.
        LanguageVersion.v2_1 -> TransactionVersion.VDev,
        // TODO(#17366): Map to TransactionVersion 2.dev once it exists.
        LanguageVersion.v2_dev -> TransactionVersion.VDev,
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
