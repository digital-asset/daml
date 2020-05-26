// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import java.net.{InetAddress, ServerSocket}

import com.daml.ports.Port

import scala.annotation.tailrec

private[postgresql] object FreePort {

  @tailrec
  def find(tries: Int = 10): PortLock.Locked = {
    val socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress)
    val portLock = try {
      val port = Port(socket.getLocalPort)
      PortLock.lock(port)
    } finally {
      socket.close()
    }
    portLock match {
      case Right(locked) =>
        socket.close()
        locked
      case Left(failure) =>
        socket.close()
        if (tries <= 1) {
          throw failure
        } else {
          find(tries - 1)
        }
    }
  }

}
