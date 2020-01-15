// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.io.File
import java.nio.file.Path

import com.daml.ledger.participant.state.kvutils.app.{Config, Runner}
import scopt.OptionParser

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

object Main extends App {
  Runner(
    "File System Ledger",
    ExtraConfig.augmentParser,
    ExtraConfig.default,
    (participantId, config: ExtraConfig) =>
      Await.result(
        FileSystemLedgerReaderWriter(
          participantId = participantId,
          root = config.root.getOrElse {
            throw new IllegalStateException("No root directory provided.")
          },
        ),
        10.seconds
    )
  ).run(args)

  case class ExtraConfig(root: Option[Path])

  object ExtraConfig {
    val default: ExtraConfig = ExtraConfig(
      root = None,
    )

    def augmentParser(parser: OptionParser[Config[ExtraConfig]]): Unit = {
      parser
        .opt[File]("directory")
        .required()
        .text("The root directory in which to store the ledger.")
        .action((file, config) => config.copy(extra = config.extra.copy(root = Some(file.toPath))))
      ()
    }
  }
}
