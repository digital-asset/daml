// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.io.File
import java.nio.file.Path

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.app.{Config, LedgerFactory, Runner}
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.resources.{ProgramResource, ResourceOwner}
import scopt.OptionParser

import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {
  new ProgramResource(Runner("File System Ledger", FileSystemLedgerFactory).owner(args)).run()

  case class ExtraConfig(root: Option[Path])

  object FileSystemLedgerFactory extends LedgerFactory[FileSystemLedgerReaderWriter, ExtraConfig] {
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

    override def owner(
        ledgerId: LedgerId,
        participantId: ParticipantId,
        config: ExtraConfig,
    )(implicit materializer: Materializer): ResourceOwner[FileSystemLedgerReaderWriter] = {
      val root = config.root.getOrElse {
        throw new IllegalStateException("No root directory provided.")
      }
      FileSystemLedgerReaderWriter.owner(
        ledgerId = ledgerId,
        participantId = participantId,
        root = root,
      )
    }
  }
}
