// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.bazeltools.clientservertest.tests

import java.io._
import java.net._

import scala.io._

object Main extends App {
  case class Conf(portFile: String, something: String)

  val argParser = new scopt.OptionParser[Conf]("server") {
    opt[String]("port-file")
      .action((x, c) => c.copy(portFile = x))
      .text("Port file")
    opt[String]("something")
      .optional()
      .action((x, c) => c.copy(something = x))
      .text("Something something")
  }
  val conf = argParser.parse(args, Conf("", "")).get
  val server = new ServerSocket(0, 0, InetAddress.getLoopbackAddress)

  println(s"Writing port number ${server.getLocalPort} to file ${conf.portFile}")
  val w = new BufferedWriter(new FileWriter(conf.portFile))
  w.write(s"${server.getLocalPort}\n")
  w.close()

  while (true) {
    val s = server.accept()
    val in = new BufferedSource(s.getInputStream()).getLines()
    val out = new PrintStream(s.getOutputStream())
    out.println(in.next())
    out.flush()
    s.close()
  }
}
