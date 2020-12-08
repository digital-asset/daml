// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EngineInfoTest extends AnyWordSpec with Matchers {

  "EngineInfo" should {

    val Seq(engineInfoStable, engineInfoDev) =
      Seq(EngineConfig.Stable, EngineConfig.Dev).map(new EngineInfo(_))

    "show supported LF, Transaction and Value versions" in {

      engineInfoStable.show shouldBe
        "DAML LF Engine supports LF versions: 1.6, 1.7, 1.8"

      engineInfoDev.show shouldBe
        "DAML LF Engine supports LF versions: 1.6, 1.7, 1.8, 1.dev"
    }

    "toString returns the same value as show" in {
      engineInfoStable.toString shouldBe engineInfoStable.show
    }
  }
}
