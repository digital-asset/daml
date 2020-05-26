// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine
import org.scalatest.{Matchers, WordSpec}

class EngineInfoTest extends WordSpec with Matchers {

  EngineInfo.getClass.getSimpleName should {
    "show supported LF, Transaction and Value versions" in {
      EngineInfo.show shouldBe
        "DAML LF Engine supports LF versions: 0, 0.dev, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.dev; Transaction versions: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10; Value versions: 1, 2, 3, 4, 5, 6, 7"
    }

    "toString returns the same value as show" in {
      EngineInfo.toString shouldBe EngineInfo.show
    }
  }
}
