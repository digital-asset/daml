// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import com.daml.lf.language.LanguageVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EngineInfoTest extends AnyWordSpec with Matchers {

  "EngineInfo" should {

    val Seq(engineInfoLegacy, engineInfoStable, engineEarlyAccess, engineInfoDev) =
      List(
        LanguageVersion.LegacyVersions,
        LanguageVersion.StableVersions,
        LanguageVersion.EarlyAccessVersions,
        LanguageVersion.DevVersions,
      ).map(versions => new EngineInfo(EngineConfig(allowedLanguageVersions = versions)))

    "show supported LF, Transaction and Value versions" in {

      engineInfoLegacy.show shouldBe
        "Daml-LF Engine supports LF versions: 1.6, 1.7, 1.8"

      engineInfoStable.show shouldBe
        "Daml-LF Engine supports LF versions: 1.6, 1.7, 1.8, 1.11, 1.12, 1.13, 1.14"

      engineEarlyAccess.show shouldBe
        "Daml-LF Engine supports LF versions: 1.6, 1.7, 1.8, 1.11, 1.12, 1.13, 1.14, 1.15"

      engineInfoDev.show shouldBe
        "Daml-LF Engine supports LF versions: 1.6, 1.7, 1.8, 1.11, 1.12, 1.13, 1.14, 1.15, 1.dev"
    }

    "toString returns the same value as show" in {
      engineInfoStable.toString shouldBe engineInfoStable.show
    }
  }
}
