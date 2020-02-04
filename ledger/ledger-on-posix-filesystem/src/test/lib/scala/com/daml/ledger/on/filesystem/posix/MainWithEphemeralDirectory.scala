// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.{Files, Path}

import akka.stream.Materializer
import com.daml.ledger.on.filesystem.posix.DeleteFiles.deleteFiles
import com.daml.ledger.participant.state.kvutils.app.LedgerFactory.SimpleLedgerFactory
import com.daml.ledger.participant.state.kvutils.app.Runner
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.resources.{ProgramResource, Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}

object MainWithEphemeralDirectory {
  def main(args: Array[String]): Unit = {
    new ProgramResource(new Runner("Ephemeral File System Ledger", TestLedgerFactory).owner(args))
      .run()
  }

  object TestLedgerFactory extends SimpleLedgerFactory[FileSystemLedgerReaderWriter] {
    override def owner(
        ledgerId: LedgerId,
        participantId: ParticipantId,
        config: Unit
    )(
        implicit executionContext: ExecutionContext,
        materializer: Materializer,
    ): ResourceOwner[FileSystemLedgerReaderWriter] =
      for {
        root <- temporaryDirectory("ledger-on-posix-filesystem-ephemeral-")
        participant <- FileSystemLedgerReaderWriter.owner(
          ledgerId = ledgerId,
          participantId = participantId,
          root = root,
        )
      } yield participant
  }

  private def temporaryDirectory(prefix: String): ResourceOwner[Path] = new ResourceOwner[Path] {
    def acquire()(implicit executionContext: ExecutionContext): Resource[Path] =
      Resource[Path](
        Future(Files.createTempDirectory(prefix)),
        directory => Future(deleteFiles(directory)),
      )
  }
}
