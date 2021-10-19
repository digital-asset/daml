// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.stream.Materializer
import com.daml.ledger.on.memory.{InMemoryLedgerStateAccess, InMemoryState, Index}
import com.daml.ledger.participant.state.kvutils.{KeyValueCommitting, VersionedOffsetBuilder}
import com.daml.ledger.participant.state.kvutils.export.{
  NoOpLedgerDataExporter,
  SubmissionInfo,
  WriteSet,
}
import com.daml.ledger.participant.state.kvutils.store.DamlStateValue
import com.daml.ledger.validator.preexecution.{
  EqualityBasedPostExecutionConflictDetector,
  PreExecutingSubmissionValidator,
  PreExecutingValidatingCommitter,
  RawKeyValuePairsWithLogEntry,
  RawPostExecutionWriter,
  RawPreExecutingCommitStrategy,
  TimeBasedWriteSetSelector,
}
import com.daml.ledger.validator.{SerializingStateReader, StateKeySerializationStrategy}
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics

import scala.concurrent.{ExecutionContext, Future}

final class RawPreExecutingCommitStrategySupport(
    metrics: Metrics
)(implicit executionContext: ExecutionContext)
    extends CommitStrategySupport[Index] {
  override val stateKeySerializationStrategy: StateKeySerializationStrategy =
    StateKeySerializationStrategy.createDefault()

  private val offsetBuilder = new VersionedOffsetBuilder(0)
  private val state = InMemoryState.empty
  private val ledgerStateAccess = new InMemoryLedgerStateAccess(offsetBuilder, state, metrics)

  // To mimic the original pre-execution as closely as possible, we use the original submission
  // record time as the current time. This effectively means that the committer thinks the
  // submission takes no time at all (0ms), which means that only submissions with invalid
  // timestamps will be out of bounds.
  private val currentSubmissionRecordTime = new AtomicReference[Instant]()
  private val postExecutionWriteSetSelector =
    new TimeBasedWriteSetSelector[
      RawPreExecutingCommitStrategy.ReadSet,
      RawKeyValuePairsWithLogEntry,
    ](now = () => currentSubmissionRecordTime.get())

  private val committer = new PreExecutingValidatingCommitter[
    Option[DamlStateValue],
    RawPreExecutingCommitStrategy.ReadSet,
    RawKeyValuePairsWithLogEntry,
  ](
    transformStateReader = SerializingStateReader(stateKeySerializationStrategy),
    validator = new PreExecutingSubmissionValidator(
      new KeyValueCommitting(new Engine(), metrics),
      new RawPreExecutingCommitStrategy(stateKeySerializationStrategy),
      metrics = metrics,
    ),
    postExecutionConflictDetector = new EqualityBasedPostExecutionConflictDetector,
    postExecutionWriteSetSelector = postExecutionWriteSetSelector,
    postExecutionWriter = new RawPostExecutionWriter,
    ledgerDataExporter = NoOpLedgerDataExporter,
  )

  override def commit(
      submissionInfo: SubmissionInfo
  )(implicit materializer: Materializer, loggingContext: LoggingContext): Future[WriteSet] = {
    val access = new WriteRecordingLedgerStateAccess(ledgerStateAccess)
    currentSubmissionRecordTime.set(submissionInfo.recordTimeInstant)
    committer
      .commit(
        submissionInfo.participantId,
        submissionInfo.correlationId,
        submissionInfo.submissionEnvelope,
        submissionInfo.recordTimeInstant,
        access,
      )
      .map(_ => access.getWriteSet)
  }

  override def newReadServiceFactory(): ReplayingReadServiceFactory =
    new LogAppendingReadServiceFactory(offsetBuilder, metrics)

  override val writeSetComparison: WriteSetComparison =
    new RawWriteSetComparison(stateKeySerializationStrategy)
}
