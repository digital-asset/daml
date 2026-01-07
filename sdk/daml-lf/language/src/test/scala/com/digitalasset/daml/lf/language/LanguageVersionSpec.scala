// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import com.digitalasset.daml.lf.language.{LanguageVersion => LV}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LanguageVersionSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  "LanguageVersion.ordering order as expected" in {

    val versionInOrder = List(
      LV.v2_1,
      LV.v2_2,
      LV.v2_dev,
    )

    val versionRank = versionInOrder.zipWithIndex.toMap

    val versions = Table("version", versionInOrder: _*)

    forEvery(versions)(v1 =>
      forEvery(versions)(v2 =>
        v1.compare(v2).sign shouldBe (versionRank(v1) compareTo versionRank(v2)).sign
      )
    )
  }

  "fromString" should {

    "recognize known versions" in {
      val testCases = Table("version", (LV.allLegacyLfVersions ++ LV.allLfVersions): _*)

      forEvery(testCases)(v => LV.fromString(v.pretty) shouldBe Right(v))
    }

    "reject invalid versions" in {
      val testCases = Table("invalid version", "1.1", "2", "14", "version", "2.1.11")

      forEvery(testCases)(s => LV.fromString(s) shouldBe a[Left[_, _]])
    }
  }

}
