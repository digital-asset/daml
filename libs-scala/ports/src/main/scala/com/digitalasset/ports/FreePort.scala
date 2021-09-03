// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import java.net.{InetAddress, ServerSocket}

import scala.io.Source
import scala.util.{Random, Try}

object FreePort {

  def find(): Port = {
    val socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress)
    try {
      Port(socket.getLocalPort)
    } finally {
      socket.close()
    }
  }

  def randomPortGen(dynamicRange: (Int, Int)): () => Int = {
    val (minPort, maxPort) = (1024, 65536)
    val minExcl = Math.min(Math.max(minPort, dynamicRange._1), maxPort)  // 32768
    val maxExcl = Math.min(Math.max(minExcl, dynamicRange._2), maxPort)  // 60999
    val numLowerPorts = minExcl - minPort  // 32768 - 1024 = 31744
    val numUpperPorts = maxPort - maxExcl  // 65536 - 60999 = 4537
    val numAvailablePorts = numLowerPorts + numUpperPorts  // 31744 + 4537 = 36281
    val gen = new Random()
    def genPort(): Int = {
      val n = gen.nextInt(numAvailablePorts)  // 0     31743  31744  36280
      if (n < numLowerPorts) {                // T     T      F      F
        n + minPort                           // 1024  32767
      } else {                                //
        n - numLowerPorts + maxExcl + 1       //              61000  65536
      }
    }
    genPort
  }

  def linuxDynamicPortRange(): Try[(Int, Int)] = Try {
    val procSource = Source.fromFile("/proc/sys/net/ipv4/ip_local_port_range")
    try {
      procSource
        .getLines()
        .map(_.trim.split("\\s+").map(_.toInt))
        .collect { case Array(min, max) => (min, max) }
        .next()
    } finally {
      procSource.close()
    }
  }

}
