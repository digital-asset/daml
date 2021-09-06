// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import java.io.IOException
import java.net.{InetAddress, ServerSocket}

import com.daml.bazeltools.BazelRunfiles.rlocation

import scala.io.Source
import scala.sys.process.Process
import scala.util.{Random, Try}

object FreePort {
  private val maxAttempts = 100
  private val dynamicRange = dynamicPortRange()

  def find(): Port = {
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

  private def randomPortGen(dynamicRange: (Int, Int)): () => Int = {
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

  private def dynamicPortRange(): (Int, Int) = {
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

  private def linuxDynamicPortRange(): Try[(Int, Int)] = Try {
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

  private def macosDynamicPortRange(): Try[(Int, Int)] = Try {
    val sysctl = rlocation("external/sysctl_nix/bin/sysctl")
    val out = Process(sysctl, Seq("net.inet.ip.portrange.first", "net.inet.ip.portrange.last")).!!
    var min: Option[Int] = None
    var max: Option[Int] = None
    out.split("\n").map(_.trim.split("\\s+")).foreach {
      case Array(name, value) if name.startsWith("net.inet.ip.portrange.first") =>
        min = Some(value.toInt)
      case Array(name, value) if name.startsWith("net.inet.ip.portrange.last") =>
        max = Some(value.toInt)
      case _ => ()
    }
    (min.get, max.get)
  }

  private def windowsDynamicPortRange(): Try[(Int, Int)] = Try {
    val out = Process("netsh int ipv4 show dynamicport tcp").!!
    var min: Option[Int] = None
    var num: Option[Int] = None
    out.split(System.lineSeparator()).map(_.trim.toLowerCase()).foreach {
      case line if line.startsWith("start port") => min = Some(line.split("\\s+").last.toInt)
      case line if line.startsWith("number of ports") => num = Some(line.split("\\s+").last.toInt)
    }
    (min.get, min.get + num.get)
  }

}
