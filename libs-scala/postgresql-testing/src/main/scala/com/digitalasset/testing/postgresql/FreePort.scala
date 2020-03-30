// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing.postgresql

import java.net.{InetAddress, ServerSocket}

import com.digitalasset.ports.Port

import scala.annotation.tailrec

private[postgresql] object FreePort {

  @tailrec
  def find(): PortLock.Locked = {
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
      case Left(PortLock.FailedToLock) =>
        socket.close()
        find()
    }
  }

}
