// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.nio.file.{Path, Paths}

import scopt.{OptionParser, Read}

object Config {

  private implicit val pathRead: Read[Path] = Read.reads(Paths.get(_))

  private implicit val readTest: Read[MigrationStep.Test] =
    Read.stringRead.map(s =>
      s.split(",", -1) match {
        case Array(Divulgence.ApplicationId, owner, divulgee, suffix) =>
          new Divulgence(owner, divulgee, suffix)
        case Array(KeyTransfer.ApplicationId, owner, receiver, suffix) =>
          new KeyTransfer(owner, receiver, suffix)
        case Array(ProposeAccept.ApplicationId, proposer, accepter, note) =>
          new ProposeAccept(proposer, accepter, note)
        case _ =>
          throw new IllegalArgumentException(s"Illegal test name or parameters '$s'")
      }
    )

  val parser: OptionParser[Config] = new scopt.OptionParser[Config]("migration-step") {
    opt[Path]("dar")
      .action((dar, c) => c.copy(dar = dar))
      .required()
    opt[String]("host")
      .action((host, c) => c.copy(host = host))
      .required()
    opt[Int]("port")
      .action((port, c) => c.copy(port = port))
      .required()
    opt[Path]("output")
      .action((path, c) => c.copy(outputFile = path))
      .required()
    opt[MigrationStep.Test]("test")
      .action((test, c) => c.copy(test = test))
      .required()
  }

  // Null-safety is provided by the CLI parser making all fields required
  val default: Config = Config(null, 0, null, null, null)

  sealed trait Test {
    def host: String
    def port: Int
    def outputFile: Path
  }

}

final case class Config(
    host: String,
    port: Int,
    outputFile: Path,
    dar: Path,
    test: MigrationStep.Test,
) extends Config.Test
