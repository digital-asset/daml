// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing.postgresql

import org.scalatest.{Matchers, WordSpec}

class FreePortSpec extends WordSpec with Matchers {
  "a free port" should {
    "always be available" in {
      val port = FreePort.find()
      port should (be >= 1024 and be < 65536)
    }
  }
}
