// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
        "DAML LF Engine supports LF versions: 1.6, 1.7, 1.8"

      engineInfoStable.show shouldBe
        "DAML LF Engine supports LF versions: 1.6, 1.7, 1.8, 1.11"

      engineEarlyAccess.show shouldBe
        "DAML LF Engine supports LF versions: 1.6, 1.7, 1.8, 1.11, 1.12"

      engineInfoDev.show shouldBe
        "DAML LF Engine supports LF versions: 1.6, 1.7, 1.8, 1.11, 1.12, 1.dev"
    }

    "toString returns the same value as show" in {
      engineInfoStable.toString shouldBe engineInfoStable.show
    }
  }
}
