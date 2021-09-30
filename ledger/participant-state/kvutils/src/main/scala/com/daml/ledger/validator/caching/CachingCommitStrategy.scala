// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.DamlKvutils
import com.daml.ledger.participant.state.kvutils.DamlState.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator
import com.daml.ledger.validator.{
  CommitStrategy,
  LedgerStateOperations,
  LogAppendingCommitStrategy,
  StateKeySerializationStrategy,
}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

final class CachingCommitStrategy[Result](
    cache: Cache[DamlStateKey, DamlStateValue],
    shouldCache: DamlStateKey => Boolean,
    delegate: CommitStrategy[Result],
)(implicit executionContext: ExecutionContext)
    extends CommitStrategy[Result] {
  override def commit(
      participantId: Ref.ParticipantId,
      correlationId: String,
      entryId: DamlKvutils.DamlLogEntryId,
      entry: DamlKvutils.DamlLogEntry,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
      outputState: Map[DamlStateKey, DamlStateValue],
      exporterWriteSet: Option[SubmissionAggregator.WriteSetBuilder],
  )(implicit loggingContext: LoggingContext): Future[Result] =
    for {
      _ <- Future {
        outputState.view.filter { case (key, _) => shouldCache(key) }.foreach { case (key, value) =>
          cache.put(key, value)
        }
      }
      result <- delegate.commit(
        participantId,
        correlationId,
        entryId,
        entry,
        inputState,
        outputState,
        exporterWriteSet,
      )
    } yield result
}

object CachingCommitStrategy {
  def apply[LogResult](
      stateCache: Cache[DamlStateKey, DamlStateValue],
      cacheUpdatePolicy: CacheUpdatePolicy[DamlStateKey],
      ledgerStateOperations: LedgerStateOperations[LogResult],
      keySerializationStrategy: StateKeySerializationStrategy,
  )(implicit executionContext: ExecutionContext): CachingCommitStrategy[LogResult] =
    new CachingCommitStrategy(
      stateCache,
      cacheUpdatePolicy.shouldCacheOnWrite,
      new LogAppendingCommitStrategy(ledgerStateOperations, keySerializationStrategy),
    )
}
