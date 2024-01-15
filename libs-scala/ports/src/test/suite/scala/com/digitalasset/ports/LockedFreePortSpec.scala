// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LockedFreePortSpec extends AnyWordSpec with Matchers {
  "a free port" should {
    "always be available" in {
      val lockedPort = LockedFreePort.find()
      try {
        lockedPort.port.value should (be >= 1024 and be < 65536)
      } finally {
        lockedPort.unlock()
      }
    }

    "not collide with OS dynamic ports" in {
      val lockedPort = LockedFreePort.find()
      val (dynMin, dynMax) = FreePort.dynamicRange
      try {
        lockedPort.port.value should (be < dynMin or be > dynMax)
      } finally {
        lockedPort.unlock()
      }
    }

    "lock, to prevent race conditions" in {
      val lockedPort = LockedFreePort.find()
      try {
        PortLock.lock(lockedPort.port) should be(Left(PortLock.FailedToLock(lockedPort.port)))
      } finally {
        lockedPort.unlock()
      }
    }

    "unlock when the server's started" in {
      val lockedPort = LockedFreePort.find()
      lockedPort.unlock()

      val locked = PortLock
        .lock(lockedPort.port)
        .fold(failure => throw failure, identity)
      locked.unlock()
      succeed
    }

    "not error if it's unlocked twice" in {
      val lockedPort = LockedFreePort.find()
      lockedPort.unlock()
      lockedPort.unlock()
      succeed
    }
  }
}
