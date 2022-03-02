// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PortSpec extends AnyWordSpec with Matchers {
  "a port" can {
    "be constructed from a valid port number" in {
      val port = Port(1024)
      port.value should be(1024)
    }

    "not be constructed from a port number greater than 65,535" in {
      an[IllegalArgumentException] should be thrownBy Port(65536)
      an[IllegalArgumentException] should be thrownBy Port(80000)
    }

    "not be constructed from a port number less than 0" in {
      an[IllegalArgumentException] should be thrownBy Port(-1)
      an[IllegalArgumentException] should be thrownBy Port(-5)
    }
  }

  "a port" should {
    "serialize to a string" in {
      Port(12345).toString should be("12345")
    }
  }
}
