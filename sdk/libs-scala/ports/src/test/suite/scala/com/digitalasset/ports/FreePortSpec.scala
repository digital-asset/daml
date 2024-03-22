// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FreePortSpec extends AnyWordSpec with Matchers {
  "a free port" should {
    "always be available" in {
      val port = FreePort.find()
      port.value should (be >= 1024 and be < 65536)
    }
  }
}
