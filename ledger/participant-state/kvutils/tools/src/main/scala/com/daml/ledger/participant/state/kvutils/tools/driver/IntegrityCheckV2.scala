// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.driver

import java.io.{BufferedInputStream, DataInputStream}
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.participant.state.kvutils.export.{
  LedgerDataExporter,
  SerializationBasedLedgerDataImporter
}
import com.daml.ledger.participant.state.kvutils.tools.color
import com.daml.ledger.participant.state.kvutils.tools.export.{
  IntegrityChecker,
  LogAppendingCommitStrategySupport
}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object IntegrityCheckV2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("usage: integrity-check <ledger dump file>")
      println(
        s"You can produce a ledger dump on a kvutils ledger by setting ${LedgerDataExporter.EnvironmentVariableName}=/path/to/file")
      sys.exit(1)
    }

    val path = Paths.get(args(0))
    println(s"Verifying integrity of $path...")

    val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(
        Executors.newFixedThreadPool(sys.runtime.availableProcessors()))
    val input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))
    val importer = new SerializationBasedLedgerDataImporter(input)
    new IntegrityChecker(LogAppendingCommitStrategySupport)
      .run(importer)(executionContext)
      .andThen {
        case _ =>
          input.close()
          executionContext.shutdown()
      }(DirectExecutionContext)
      .failed
      .foreach {
        case exception: IntegrityChecker.CheckFailedException =>
          println(exception.getMessage.red)
          sys.exit(1)
        case exception =>
          exception.printStackTrace()
          sys.exit(1)
      }(DirectExecutionContext)
  }
}
