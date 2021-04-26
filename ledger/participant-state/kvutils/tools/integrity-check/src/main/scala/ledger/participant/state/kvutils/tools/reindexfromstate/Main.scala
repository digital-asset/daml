package com.daml.ledger.participant.state.kvutils.tools.reindexfromstate

import com.daml.ledger.participant.state.kvutils.tools.integritycheck.LogAppendingCommitStrategySupport
import com.daml.metrics.Metrics

import scala.concurrent.ExecutionContext

object Main {
  def main(args: Array[String]): Unit = {
    Reindexer.runAndExit(args, batchingCommitStrategySupportFactory _)
  }

  private def batchingCommitStrategySupportFactory(
                                            metrics: Metrics,
                                            executionContext: ExecutionContext,
                                          ) = new LogAppendingCommitStrategySupport(metrics)(executionContext)
}
