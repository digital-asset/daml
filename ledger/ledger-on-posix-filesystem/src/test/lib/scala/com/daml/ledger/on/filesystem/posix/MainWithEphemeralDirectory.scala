// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.Files

import com.daml.ledger.on.filesystem.posix.DeleteFiles.deleteFiles
import com.daml.ledger.participant.state.kvutils.app.Runner

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

object MainWithEphemeralDirectory extends App {
  val root = Files.createTempDirectory("ledger-on-posix-filesystem-ephemeral")

  try {
    Runner(
      "Ephemeral File System Ledger",
      (ledgerId, participantId) =>
        Await.result(
          FileSystemLedgerReaderWriter(
            ledgerId = ledgerId,
            participantId = participantId,
            root = root,
          ),
          10.seconds)
    ).run(args)
  } finally {
    deleteFiles(root)
  }
}
