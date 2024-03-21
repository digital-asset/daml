// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.language.LanguageVersion
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TransactionVersionSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import LanguageVersion.{v1_6, v1_7, v1_8, v1_11, v1_dev}
  import TransactionVersion.{V10, V11, VDev}

  "TransactionVersion.assignNodeVersion" should {

    val testCases = Table(
      "language version" -> "transaction version",
      v1_6 -> V10,
      v1_7 -> V10,
      v1_8 -> V10,
      v1_11 -> V11,
      v1_dev -> VDev,
    )

    "be stable" in {
      forEvery(testCases) { (languageVersion, transactionVersions) =>
        TransactionVersion.assignNodeVersion(languageVersion) shouldBe transactionVersions
      }
    }

  }
}
