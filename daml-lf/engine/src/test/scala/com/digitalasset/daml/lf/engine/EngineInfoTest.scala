// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine
import com.github.ghik.silencer.silent
import org.scalatest.{Matchers, WordSpec}

class EngineInfoTest extends WordSpec with Matchers {

  "EngineInfo" should {

    @silent("Sandbox_Classic in object EngineConfig is deprecated")
    def infos =
      Seq(EngineConfig.Stable, EngineConfig.Dev, EngineConfig.Sandbox_Classic)
        .map(new EngineInfo(_))

    val Seq(engineInfoStable, engineInfoDev, engineInfoLegacy) = infos

    "show supported LF, Transaction and Value versions" in {

      engineInfoStable.show shouldBe
        "DAML LF Engine supports LF versions: 0, 0.dev, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.dev; Input Transaction versions: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11; Input Value versions: 1, 2, 3, 4, 5, 6, 7; Output Transaction versions: 10; Output Value versions: 6"

      engineInfoDev.show shouldBe
        "DAML LF Engine supports LF versions: 0, 0.dev, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.dev; Input Transaction versions: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11; Input Value versions: 1, 2, 3, 4, 5, 6, 7; Output Transaction versions: 10, 11; Output Value versions: 6, 7"

      engineInfoLegacy.show shouldBe
        "DAML LF Engine supports LF versions: 0, 0.dev, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.dev; Input Transaction versions: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11; Input Value versions: 1, 2, 3, 4, 5, 6, 7; Output Transaction versions: 10, 11; Output Value versions: 6, 7"

    }

    "toString returns the same value as show" in {
      engineInfoStable.toString shouldBe engineInfoStable.show
    }
  }
}
