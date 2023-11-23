// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.language.{LanguageVersion => LV}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LanguageVersionSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  "LanguageVersion.ordering order as expected" in {

    val versionInOrder = List(
      LV(LV.Major.V1, LV.Minor("6")),
      LV(LV.Major.V1, LV.Minor("7")),
      LV(LV.Major.V1, LV.Minor("8")),
      LV(LV.Major.V1, LV.Minor("dev")),
    )

    val versionRank = versionInOrder.zipWithIndex.toMap

    val versions = Table("version", versionInOrder: _*)

    forEvery(versions)(v1 =>
      forEvery(versions)(v2 =>
        LV.Ordering
          .compare(v1, v2)
          .sign shouldBe (versionRank(v1) compareTo versionRank(v2)).sign
      )
    )
  }

  "fromString" should {

    "recognize known versions" in {
      val testCases = Table("version", LV.All: _*)

      forEvery(testCases)(v => LV.fromString(v.pretty) shouldBe Right(v))
    }

    "reject invalid versions" in {
      val testCases = Table("invalid version", "1", "14", "version", "1.11.11")

      forEvery(testCases)(s => LV.fromString(s) shouldBe a[Left[_, _]])
    }
  }

}
