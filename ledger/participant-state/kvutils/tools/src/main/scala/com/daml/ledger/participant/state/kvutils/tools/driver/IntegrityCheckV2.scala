package com.daml.ledger.participant.state.kvutils.tools.driver

import java.io.{DataInputStream, FileInputStream}
import java.util.concurrent.Executors

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.participant.state.kvutils.export.LedgerDataExporter
import com.daml.ledger.participant.state.kvutils.tools.{
  IntegrityChecker,
  LogAppendingCommitStrategySupport,
  _
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

    val filename = args(0)
    println(s"Verifying integrity of $filename...")

    val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(
        Executors.newFixedThreadPool(sys.runtime.availableProcessors()))
    val ledgerDumpStream = new DataInputStream(new FileInputStream(filename))
    new IntegrityChecker(LogAppendingCommitStrategySupport)
      .run(ledgerDumpStream)(executionContext)
      .andThen {
        case _ =>
          ledgerDumpStream.close()
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
