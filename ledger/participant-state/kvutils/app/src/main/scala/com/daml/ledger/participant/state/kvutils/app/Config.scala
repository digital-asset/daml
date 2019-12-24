// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.io.File
import java.nio.file.Path

import com.daml.ledger.participant.state.v1.ParticipantId
import scopt.OptionParser

case class Config[Extra](
    participantId: ParticipantId,
    port: Int,
    portFile: Option[Path],
    archiveFiles: Seq[Path],
    extra: Extra,
)

object Config {
  val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024

  def default[Extra](extra: Extra): Config[Extra] =
    Config(
      participantId = ParticipantId.assertFromString("example"),
      port = 6865,
      portFile = None,
      archiveFiles = Vector.empty,
      extra = extra,
    )

  def parse[Extra](
      name: String,
      extraOptions: OptionParser[Config[Extra]] => Unit,
      defaultExtra: Extra,
      args: Seq[String],
  ): Option[Config[Extra]] =
    parser(name, extraOptions).parse(args, default(defaultExtra))

  private def parser[Extra](
      name: String,
      extraOptions: OptionParser[Config[Extra]] => Unit,
  ): OptionParser[Config[Extra]] = {
    val parser = new OptionParser[Config[Extra]](name) {
      head(name)

      opt[String](name = "participant-id")
        .optional()
        .text("The participant ID given to all components of the ledger API server.")
        .action((participantId, config) =>
          config.copy(participantId = ParticipantId.assertFromString(participantId)))

      opt[Int]("port")
        .optional()
        .text("The port on which to run the ledger API server.")
        .action((port, config) => config.copy(port = port))
        .withFallback(() => 6865)

      opt[File]("port-file")
        .optional()
        .text("File to write the allocated port number to. Used to inform clients in CI about the allocated port.")
        .action((file, config) => config.copy(portFile = Some(file.toPath)))

      arg[File]("<archive>...")
        .optional()
        .unbounded()
        .text("DAR files to load. Scenarios are ignored. The server starts with an empty ledger by default.")
        .action((file, config) => config.copy(archiveFiles = config.archiveFiles :+ file.toPath))

      help("help").text("Runs the ledger as a server application.")
    }
    extraOptions(parser)
    parser
  }
}
