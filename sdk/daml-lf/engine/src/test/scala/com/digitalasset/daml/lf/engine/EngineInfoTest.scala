// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
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
        LanguageVersion.StableVersions(LanguageMajorVersion.V2),
        LanguageVersion.EarlyAccessVersions(LanguageMajorVersion.V2),
        LanguageVersion.AllVersions(LanguageMajorVersion.V2),
      ).map(versions => new EngineInfo(EngineConfig(allowedLanguageVersions = versions)))

    "show supported LF, Transaction and Value versions" in {

      engineInfoStable.show shouldBe
        "Daml-LF Engine supports LF versions: 2.1"

      engineEarlyAccess.show shouldBe
        "Daml-LF Engine supports LF versions: 2.1"

      engineInfoV2.show shouldBe
        "Daml-LF Engine supports LF versions: 2.1, 2.dev"
    }

    "toString returns the same value as show" in {
      engineInfoStable.toString shouldBe engineInfoStable.show
    }
  }
}
