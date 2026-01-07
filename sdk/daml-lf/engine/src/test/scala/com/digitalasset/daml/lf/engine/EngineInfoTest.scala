// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.language.LanguageVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EngineInfoTest extends AnyWordSpec with Matchers {

  "EngineInfo" should {

    val Seq(
      engineInfoStable,
      engineEarlyAccess,
      engineInfoV2,
    ) =
      List(
        LanguageVersion.stableLfVersionsRange,
        LanguageVersion.earlyAccessLfVersionsRange,
        LanguageVersion.allLfVersionsRange,
      ).map(versions => new EngineInfo(EngineConfig(allowedLanguageVersions = versions)))

    "show supported LF, Transaction and Value versions" in {

      engineInfoStable.show shouldBe
        "Daml-LF Engine supports LF versions: 2.1, 2.2"

      engineEarlyAccess.show shouldBe
        "Daml-LF Engine supports LF versions: 2.1, 2.2"

      engineInfoV2.show shouldBe
        "Daml-LF Engine supports LF versions: 2.1, 2.2, 2.dev"
    }

    "toString returns the same value as show" in {
      engineInfoStable.toString shouldBe engineInfoStable.show
    }
  }
}
