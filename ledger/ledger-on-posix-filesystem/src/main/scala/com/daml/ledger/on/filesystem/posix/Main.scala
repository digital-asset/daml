// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.io.File
import java.nio.file.Path

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.app.{
  Config,
  KeyValueLedger,
  LedgerConstructor,
  Runner
}
import com.daml.ledger.participant.state.v1.ParticipantId
import scopt.OptionParser

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

object Main extends App {
  Runner("File System Ledger", FileSystemLedgerConstructor).run(args)

  case class ExtraConfig(root: Option[Path])

  object FileSystemLedgerConstructor extends LedgerConstructor[ExtraConfig] {
    override val defaultExtraConfig: ExtraConfig = ExtraConfig(
      root = None,
    )

    override def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit = {
      parser
        .opt[File]("directory")
        .required()
        .text("The root directory in which to store the ledger.")
        .action((file, config) => config.copy(extra = config.extra.copy(root = Some(file.toPath))))
      ()
    }

    override def apply(participantId: ParticipantId, config: ExtraConfig)(
        implicit materializer: Materializer,
    ): KeyValueLedger =
      Await.result(
        FileSystemLedgerReaderWriter(
          participantId = participantId,
          root = config.root.getOrElse {
            throw new IllegalStateException("No root directory provided.")
          },
        ),
        10.seconds
      )
  }
}
