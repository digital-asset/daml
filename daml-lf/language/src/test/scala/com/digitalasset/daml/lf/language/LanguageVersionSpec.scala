// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.language.{LanguageMajorVersion => LVM, LanguageVersion => LV}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LanguageVersionSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  "LanguageVersion.ordering order as expected" in {

    val versionInOrder = List(
      LV(LVM.V1, "6"),
      LV(LVM.V1, "7"),
      LV(LVM.V1, "8"),
    )

    val versionRank = versionInOrder.zipWithIndex.toMap

    val versions = Table("version", versionInOrder: _*)

    forEvery(versions)(
      v1 =>
        forEvery(versions)(
          v2 =>
            LV.ordering
              .compare(v1, v2)
              .signum shouldBe (versionRank(v1) compareTo versionRank(v2)).signum))

  }

}
