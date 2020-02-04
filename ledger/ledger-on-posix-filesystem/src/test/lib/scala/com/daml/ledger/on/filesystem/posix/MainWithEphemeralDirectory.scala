// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.{Files, Path}

import com.daml.ledger.on.filesystem.posix.DeleteFiles.deleteFiles
import com.daml.ledger.participant.state.kvutils.app.Runner
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.resources.{ProgramResource, Resource, ResourceOwner}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

object MainWithEphemeralDirectory extends App {
  new ProgramResource(Runner("Ephemeral File System Ledger", owner _).owner(args)).run()

  def owner(
      ledgerId: LedgerId,
      participantId: ParticipantId,
  ): ResourceOwner[FileSystemLedgerReaderWriter] =
    for {
      root <- temporaryDirectory("ledger-on-posix-filesystem-ephemeral-")
      participant <- FileSystemLedgerReaderWriter.owner(
        ledgerId = ledgerId,
        participantId = participantId,
        root = root,
      )
    } yield participant

  def temporaryDirectory(prefix: String): ResourceOwner[Path] = new ResourceOwner[Path] {
    def acquire()(implicit executionContext: ExecutionContext): Resource[Path] =
      Resource[Path](
        Future(Files.createTempDirectory(prefix))(executionContext),
        directory => Future(deleteFiles(directory))(executionContext),
      )(executionContext)
  }
}
