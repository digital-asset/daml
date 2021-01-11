// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import com.daml.ports

import scala.annotation.tailrec

object LockedFreePort {

  @tailrec
  def find(tries: Int = 10): PortLock.Locked = {
    val port = ports.FreePort.find()
    PortLock.lock(port) match {
      case Right(locked) =>
        locked
      case Left(failure) =>
        if (tries <= 1) {
          throw failure
        } else {
          find(tries - 1)
        }
    }
  }

}
