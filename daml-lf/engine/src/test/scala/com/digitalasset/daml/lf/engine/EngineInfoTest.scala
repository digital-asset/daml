// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import com.github.ghik.silencer.silent
import org.scalatest.{Matchers, WordSpec}

class EngineInfoTest extends WordSpec with Matchers {

  "EngineInfo" should {

    @silent("SandboxClassicDev in object EngineConfig is deprecated")
    def infos =
      Seq(EngineConfig.Stable, EngineConfig.Dev, EngineConfig.SandboxClassicDev)
        .map(new EngineInfo(_))

    val Seq(engineInfoStable, engineInfoDev, engineInfoLegacy) = infos

    "show supported LF, Transaction and Value versions" in {

      infos.foreach(
        _.pretty.toSeq(0) shouldBe
          "DAML LF Engine supports LF versions: 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.dev; input transaction versions: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11; input value versions: 1, 2, 3, 4, 5, 6, 7; output transaction versions: 10, 11; output value versions: 6, 7."
      )
    }

    "show allowed LF, Transaction and Value versions" in {

      engineInfoStable.pretty.toSeq(1) shouldBe
        "DAML LF Engine config allows LF versions: 1.6, 1.7, 1.8; input transaction versions: 10; input value versions: 6; output transaction versions: 10; output value versions: 6."

      engineInfoDev.pretty.toSeq(1) shouldBe
        "DAML LF Engine config allows LF versions: 1.6, 1.7, 1.8, 1.dev; input transaction versions: 10, 11; input value versions: 6, 7; output transaction versions: 10, 11; output value versions: 6, 7."

      engineInfoLegacy.pretty.toSeq(1) shouldBe
        "DAML LF Engine config allows LF versions: 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.dev; input transaction versions: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11; input value versions: 1, 2, 3, 4, 5, 6, 7; output transaction versions: 10, 11; output value versions: 6, 7."

    }

    "toString returns the same value as show" in {
      engineInfoStable.toString shouldBe engineInfoStable.show
    }
  }
}
