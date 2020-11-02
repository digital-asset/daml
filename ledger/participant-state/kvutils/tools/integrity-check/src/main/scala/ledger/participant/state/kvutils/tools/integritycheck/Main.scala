// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.util.concurrent.Executors

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.on.memory.Index
import com.daml.ledger.participant.state.kvutils.export.ProtobufBasedLedgerDataImporter

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

object Main {
  def main(args: Array[String]): Unit = {
    val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(
        Executors.newFixedThreadPool(sys.runtime.availableProcessors()))

    run(
      args,
      new LogAppendingCommitStrategySupport()(executionContext),
      () => executionContext.shutdown(),
    )
  }

  def run(
      args: Array[String],
      commitStrategySupport: CommitStrategySupport[Index],
      cleanup: () => Unit,
  ): Unit = {
    val config = Config.parse(args).getOrElse { sys.exit(1) }

    run(config, commitStrategySupport, cleanup).failed
      .foreach {
        case exception: IntegrityChecker.CheckFailedException =>
          println(exception.getMessage.red)
          sys.exit(1)
        case exception =>
          exception.printStackTrace()
          sys.exit(1)
      }(DirectExecutionContext)
  }

  private def run(
      config: Config,
      commitStrategySupport: CommitStrategySupport[Index],
      cleanup: () => Unit,
  ): Future[Unit] = {
    println(s"Verifying integrity of ${config.exportFilePath}...")

    val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(
        Executors.newFixedThreadPool(sys.runtime.availableProcessors()))
    val importer = ProtobufBasedLedgerDataImporter(config.exportFilePath)
    new IntegrityChecker(commitStrategySupport)
      .run(importer, config)(executionContext)
      .andThen {
        case _ =>
          importer.close()
          executionContext.shutdown()
          cleanup()
      }(DirectExecutionContext)
  }
}
