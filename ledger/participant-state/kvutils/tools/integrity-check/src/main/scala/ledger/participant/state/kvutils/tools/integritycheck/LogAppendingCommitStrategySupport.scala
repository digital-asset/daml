// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.stream.Materializer
import com.daml.ledger.on.memory.{InMemoryLedgerStateAccess, InMemoryState, Index}
import com.daml.ledger.participant.state.kvutils.export.{
  NoOpLedgerDataExporter,
  SubmissionInfo,
  WriteSet,
}
import com.daml.ledger.participant.state.kvutils.{KVOffsetBuilder, KeyValueCommitting}
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.ledger.validator.batch.{
  BatchedSubmissionValidator,
  BatchedSubmissionValidatorFactory,
  BatchedSubmissionValidatorParameters,
  ConflictDetection,
}
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics

import scala.concurrent.{ExecutionContext, Future}

final class LogAppendingCommitStrategySupport(
    metrics: Metrics
)(implicit executionContext: ExecutionContext)
    extends CommitStrategySupport[Index] {
  private val offsetBuilder = new KVOffsetBuilder(0)
  private val state = InMemoryState.empty

  private val serializationStrategy = StateKeySerializationStrategy.createDefault()

  private val engine = new Engine()

  private val submissionValidator = BatchedSubmissionValidator[Index](
    params = BatchedSubmissionValidatorParameters(cpuParallelism = 1, readParallelism = 1),
    committer = new KeyValueCommitting(engine, metrics),
    conflictDetection = new ConflictDetection(metrics),
    metrics = metrics,
    ledgerDataExporter = NoOpLedgerDataExporter,
  )

  override val stateKeySerializationStrategy: StateKeySerializationStrategy =
    serializationStrategy

  override def commit(
      submissionInfo: SubmissionInfo
  )(implicit materializer: Materializer, loggingContext: LoggingContext): Future[WriteSet] = {
    val access = new WriteRecordingLedgerStateAccess(
      new InMemoryLedgerStateAccess(offsetBuilder, state, metrics)
    )
    access.inTransaction { operations =>
      val (ledgerStateReader, commitStrategy) =
        BatchedSubmissionValidatorFactory.readerAndCommitStrategyFrom(
          operations,
          serializationStrategy,
        )
      submissionValidator
        .validateAndCommit(
          submissionInfo.submissionEnvelope,
          submissionInfo.correlationId,
          submissionInfo.recordTime,
          submissionInfo.participantId,
          ledgerStateReader,
          commitStrategy,
        )
        .map(_ => access.getWriteSet)
    }
  }

  override def newReadServiceFactory(): ReplayingReadServiceFactory =
    new LogAppendingReadServiceFactory(offsetBuilder, metrics)

  override val writeSetComparison: WriteSetComparison =
    new RawWriteSetComparison(serializationStrategy)
}
