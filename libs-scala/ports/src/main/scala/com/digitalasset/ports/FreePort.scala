// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ports

import java.net.{InetAddress, ServerSocket}

object FreePort {
  def find(): Int = {
    val socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress)
    try {
      socket.getLocalPort
    } finally {
      // We have to release the port so that it can be used. Note that there is a small race window,
      // as releasing the port then handing it to the server is not atomic. If this turns out to be
      // an issue, we need to find an atomic way of doing that.
      socket.close()
    }
  }
}
