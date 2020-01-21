// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.io.File
import java.nio.file.Path

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.app.{Config, KeyValueLedger, LedgerFactory, Runner}
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import scopt.OptionParser

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

object Main extends App {
  Runner("File System Ledger", FileSystemLedgerFactory).run(args)

  case class ExtraConfig(root: Option[Path])

  object FileSystemLedgerFactory extends LedgerFactory[ExtraConfig] {
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

    override def apply(ledgerId: LedgerId, participantId: ParticipantId, config: ExtraConfig)(
        implicit materializer: Materializer,
    ): KeyValueLedger = {
      val root = config.root.getOrElse {
        throw new IllegalStateException("No root directory provided.")
      }
      Await.result(
        FileSystemLedgerReaderWriter(
          ledgerId = ledgerId,
          participantId = participantId,
          root = root,
        ),
        10.seconds,
      )
    }
  }
}
