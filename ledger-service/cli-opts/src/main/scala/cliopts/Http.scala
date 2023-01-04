// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.cliopts

import scalaz.syntax.id._
import scalaz.syntax.std.option._

import java.io.File
import java.nio.file.Path

object Http {
  val defaultAddress: String = java.net.InetAddress.getLoopbackAddress.getHostAddress

  /** Add options for setting up an HTTP server to `parser`.
    *
    * @param defaultHttpPort
    *     If set, http-port is optional, with default given; otherwise the option is required.
    * @param portFile
    *     If set, there will be an optional port-file option; otherwise it will be absent.
    */
  def serverParse[C](parser: scopt.OptionParser[C], serviceName: String)(
      address: Setter[C, String],
      httpPort: Setter[C, Int],
      defaultHttpPort: Option[Int],
      portFile: Option[Setter[C, Option[Path]]],
  ): Unit = {
    import parser.opt

    opt[String]("address")
      .action((x, c) => address(_ => x, c))
      .optional()
      .text(
        s"IP address that $serviceName service listens on. Defaults to ${defaultAddress: String}."
      )

    opt[Int]("http-port")
      .action((x, c) => httpPort(_ => x, c))
      .into(o => if (defaultHttpPort.isDefined) o.optional() else o.required())
      .text(
        s"$serviceName service port number. " +
          defaultHttpPort.cata(p => s"Defaults to ${p: Int}. ", "") +
          "A port number of 0 will let the system pick an ephemeral port." +
          (if (portFile.isDefined) " Consider specifying `--port-file` option with port number 0."
           else "")
      )

    portFile foreach { setPortFile =>
      opt[File]("port-file")
        .action((x, c) => setPortFile(_ => Some(x.toPath), c))
        .optional()
        .text(
          "Optional unique file name where to write the allocated HTTP port number. " +
            "If process terminates gracefully, this file will be deleted automatically. " +
            s"Used to inform clients in CI about which port $serviceName listens on. " +
            "Defaults to none, that is, no file gets created."
        )
    }
  }
}
