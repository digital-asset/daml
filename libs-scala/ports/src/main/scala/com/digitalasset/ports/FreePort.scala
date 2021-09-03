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
    val minExcl = Math.min(Math.max(minPort, dynamicRange._1), maxPort)
    val maxExcl = Math.min(Math.max(minExcl, dynamicRange._2), maxPort)
    val numLowerPorts = minExcl - minPort
    val numUpperPorts = maxPort - maxExcl
    val numAvailablePorts = numLowerPorts + numUpperPorts
    val gen = new Random()
    def genPort(): Int = {
      val n = gen.nextInt(numAvailablePorts)
      if (n < numLowerPorts) {
        n + minPort
      } else {
        n - numLowerPorts + maxExcl + 1
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
