// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import java.io.IOException
import java.net.{InetAddress, ServerSocket}

import com.daml.bazeltools.BazelRunfiles.rlocation

import scala.io.Source
import scala.sys.process.Process
import scala.util.{Failure, Try}

object FreePort {
  private val maxAttempts = 100
  private[ports] val dynamicRange = dynamicPortRange()

  /** Find a free port.
    *
    * This determines whether a port is free by attempting to bind it and immediately releasing it again.
    * Note, this does not guarantee that the port is still free after this function returns, see [[LockedFreePort]].
    *
    * Draws ports from outside the dynamic port range (port 0)
    * to avoid collisions with parts of the code base that directly bind to port 0.
    */
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
      .getOrElse(throw NoFreePortException)
  }

  sealed abstract class FindPortException(message: String, cause: Throwable = null)
      extends RuntimeException(message, cause)

  final case object NoFreePortException extends FindPortException("No free port found")

  final case class DynamicPortRangeException(cause: Throwable)
      extends FindPortException("Failed to determine the dynamic port range", cause)

  /** Generate a random port number outside of the dynamic port range.
    */
  private def randomPortGen(dynamicRange: (Int, Int)): () => Int = {
    val (minPort, maxPort) = (1024, 65536)
    // Exclude the dynamic port range (cropped to within the valid port range).
    // E.g. 32768 60999 on most Linux systems.
    val minExcl = Math.min(Math.max(minPort, dynamicRange._1), maxPort)
    val maxExcl = Math.min(Math.max(minExcl, dynamicRange._2), maxPort)
    val numLowerPorts = minExcl - minPort
    val numUpperPorts = maxPort - maxExcl
    val numAvailablePorts = numLowerPorts + numUpperPorts
    // generate a random number within [0, numAvailablePorts)
    // and map it to [minPort, minExcl) \cup (maxExcl, maxPort].
    def genPort(): Int = {
      val n = scala.util.Random.nextInt(numAvailablePorts)
      if (n < numLowerPorts) {
        n + minPort
      } else {
        n - numLowerPorts + maxExcl + 1
      }
    }
    () => genPort()
  }

  private def dynamicPortRange(): (Int, Int) = {
    def wrapError[A](a: Try[A]): A = a.recoverWith { case e =>
      Failure(DynamicPortRangeException(e))
    }.get
    sys.props("os.name") match {
      case os if os.toLowerCase.contains("windows") =>
        wrapError(windowsDynamicPortRange())
      case os if os.toLowerCase.contains("mac") =>
        wrapError(macosDynamicPortRange())
      case os if os.toLowerCase.contains("linux") =>
        wrapError(linuxDynamicPortRange())
      case os =>
        throw DynamicPortRangeException(new RuntimeException(s"Unsupported operating system: $os"))
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
      case _ => ()
    }
    (min.get, min.get + num.get)
  }

}
