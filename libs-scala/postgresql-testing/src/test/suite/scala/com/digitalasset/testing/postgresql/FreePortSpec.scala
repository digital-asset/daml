// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing.postgresql

import org.scalatest.{Matchers, WordSpec}

class FreePortSpec extends WordSpec with Matchers {
  "a free port" should {
    "always be available" in {
      val lockedPort = FreePort.find()
      try {
        lockedPort.port.value should (be >= 1024 and be < 65536)
      } finally {
        lockedPort.unlock()
      }
    }

    "lock, to prevent race conditions" in {
      val lockedPort = FreePort.find()
      try {
        PortLock.lock(lockedPort.port) should be(Left(PortLock.FailedToLock))
      } finally {
        lockedPort.unlock()
      }
    }

    "unlock when the server's started" in {
      val lockedPort = FreePort.find()
      lockedPort.unlock()

      val lock = PortLock.lock(lockedPort.port)
      lock match {
        case Right(locked) =>
          locked.unlock()
          succeed
        case Left(PortLock.FailedToLock) =>
          fail("Failed to lock the port.")
      }
    }

    "can be unlocked twice" in {
      val lockedPort = FreePort.find()
      lockedPort.unlock()
      lockedPort.unlock()
      succeed
    }
  }
}
