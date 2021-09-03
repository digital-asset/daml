// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import java.io.IOException
import java.net.{InetAddress, ServerSocket}

import scala.io.Source
import scala.util.{Random, Try}

object FreePort {

  def find(): Port = {
    val maxAttempts = 100
    val dynamicRange = dynamicPortRange()
    val portGen = randomPortGen(dynamicRange)
    val portCandidates = (1 to maxAttempts).map(_ => portGen())
    portCandidates
      .find { candidate =>
        try {
          val socket = new ServerSocket(candidate, 0, InetAddress.getLoopbackAddress)
          socket.close()
          true
        } catch {
          case _: IOException => false
        }
      }
      .map(found => Port(found))
      .get
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

  def dynamicPortRange(): (Int, Int) = {
    sys.props("os.name") match {
      case os if os.toLowerCase.contains("windows") =>
        windowsDynamicPortRange().get
      case os if os.toLowerCase.contains("mac") =>
        macosDynamicPortRange().get
      case os if os.toLowerCase.contains("linux") =>
        linuxDynamicPortRange().get
      case os =>
        throw new RuntimeException(s"Unsupported operating system: $os")
    }
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

  def macosDynamicPortRange(): Try[(Int, Int)] = {
    // TODO[AH] Implement dynamic port range detection on MacOS
    //   https://stackoverflow.com/questions/46023485/what-is-the-range-of-ephemeral-ports-on-mac
    throw new NotImplementedError("dynamic port range detection on MacOS")
  }

  def windowsDynamicPortRange(): Try[(Int, Int)] = {
    // TODO[AH] Implement dynamic port range detection on Windows
    //   https://docs.microsoft.com/en-us/troubleshoot/windows-server/networking/default-dynamic-port-range-tcpip-chang
    throw new NotImplementedError("dynamic port range detection on Windows")
  }

}
