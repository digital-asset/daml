// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import java.nio.file.{Path}
import java.io.File

final case class Config(
    ledgerHost: String,
    ledgerPort: Int,
    parties: List[String],
    outputPath: Path,
    sdkVersion: String,
    damlScriptLib: String,
)

object Config {
  def parse(args: Array[String]): Option[Config] =
    parser.parse(args, Empty)

  private val parser = new scopt.OptionParser[Config]("script-dump") {
    opt[String]("host")
      .required()
      .action((x, c) => c.copy(ledgerHost = x))
    opt[Int]("port")
      .required()
      .action((x, c) => c.copy(ledgerPort = x))
    opt[String]("party")
      .required()
      .unbounded()
      .action((x, c) => c.copy(parties = x :: c.parties))
    opt[File]('o', "output")
      .required()
      .action((x, c) => c.copy(outputPath = x.toPath))
    opt[String]("sdk-version")
      .required()
      .action((x, c) => c.copy(sdkVersion = x))
  }

  private val Empty = Config(
    ledgerHost = "",
    ledgerPort = -1,
    parties = List(),
    outputPath = null,
    sdkVersion = "",
    damlScriptLib = "daml-script",
  )
}
