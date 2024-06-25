// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.language.LanguageVersion
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TransactionVersionSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  "TransactionVersion.assignNodeVersion" should {

    "be stable" in {
      val testCases = Table(
        "language version" -> "transaction version",
        LanguageVersion.v2_1 -> TransactionVersion.V31,
        LanguageVersion.v2_dev -> TransactionVersion.VDev,
      )

      forEvery(testCases) { (languageVersion, transactionVersions) =>
        TransactionVersion.assignNodeVersion(languageVersion) shouldBe transactionVersions
      }
    }

    "be total" in {
      LanguageVersion.AllV2.foreach(TransactionVersion.assignNodeVersion(_))
    }

    "surjective" in {
      LanguageVersion.AllV2
        .map(TransactionVersion.assignNodeVersion(_))
        .toSet shouldBe TransactionVersion.All.toSet
    }

  }
}
