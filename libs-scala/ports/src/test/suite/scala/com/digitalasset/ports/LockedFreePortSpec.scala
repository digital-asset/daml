// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import java.net.{InetAddress, ServerSocket}

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

    "not collide with OS dynamic ports" in {
      // This test is inherently flaky in that it can produce false positives,
      // i.e. can succeed even though the issue it is testing for persists.
      // Increase this number if you find the test to yield too many false positives.
      val num = 200
      val locks: Seq[PortLock.Locked] = (1 to num).map(_ => LockedFreePort.find())
      val lockedPorts: Set[Int] = locks.map(_.port.value).toSet
      val sockets: Seq[ServerSocket] =
        (1 to num).map(_ => new ServerSocket(0, 0, InetAddress.getLoopbackAddress))
      val socketPorts: Set[Int] = sockets.map(_.getLocalPort).toSet
      try {
        lockedPorts.intersect(socketPorts) shouldBe empty
      } finally {
        locks.foreach(_.unlock())
        sockets.foreach(_.close())
      }
    }
  }
}
