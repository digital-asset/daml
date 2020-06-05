// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.nio.file.{Path, Paths}

import scopt.Read

object Config {

  private implicit val pathRead: Read[Path] = Read.reads(Paths.get(_))

  val parser = new scopt.OptionParser[Config]("migration-step") {
    opt[Path]("output")
      .action((path, c) => c.copy(outputFile = path))
      .required()
    opt[Path]("dar")
      .action((dar, c) => c.copy(dar = dar))
      .required()
    opt[String]("host")
      .action((host, c) => c.copy(host = host))
      .required()
    opt[Int]("port")
      .action((port, c) => c.copy(port = port))
      .required()
    opt[String]("proposer")
      .action((proposer, c) => c.copy(proposer = proposer))
      .required()
    opt[String]("accepter")
      .action((accepter, c) => c.copy(accepter = accepter))
      .required()
    opt[String]("note")
      .action((note, c) => c.copy(note = note))
      .required()
  }

  // Null-safety is provided by the CLI parser making all fields required
  val default: Config = Config(null, null, null, 0, null, null, null)

}

final case class Config(
    outputFile: Path,
    dar: Path,
    host: String,
    port: Int,
    proposer: String,
    accepter: String,
    note: String,
)
