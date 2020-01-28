// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.{Files, Path}

import com.daml.ledger.on.filesystem.posix.DeleteFiles.deleteFiles
import com.daml.ledger.participant.state.kvutils.app.Runner
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.resources.Resource

import scala.concurrent.{ExecutionContext, Future}

object MainWithEphemeralDirectory extends App {
  implicit val executionContext: ExecutionContext = DirectExecutionContext

  val root = Files.createTempDirectory("ledger-on-posix-filesystem-ephemeral-")

  for {
    root <- Resource[Path](
      Future.successful(root),
      directory => Future.successful(deleteFiles(directory)),
    )
    _ <- Runner(
      "Ephemeral File System Ledger",
      (ledgerId, participantId) =>
        FileSystemLedgerReaderWriter.owner(
          ledgerId = ledgerId,
          participantId = participantId,
          root = root,
        ),
    ).run(args)
  } yield ()
}
