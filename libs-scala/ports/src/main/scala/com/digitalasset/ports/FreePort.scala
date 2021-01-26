// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import java.net.{InetAddress, ServerSocket}

object FreePort {

  def find(): Port = {
    val socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress)
    try {
      Port(socket.getLocalPort)
    } finally {
      socket.close()
    }
  }

}
