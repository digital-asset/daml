// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.DamlState.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.validator.caching.{
  CacheUpdatePolicy,
  CachingCommitStrategy,
  CachingStateReader,
}
import com.daml.ledger.validator.reading.{DamlLedgerStateReader, LedgerStateReader}
import com.daml.ledger.validator.{
  CommitStrategy,
  DamlLedgerStateReader,
  DefaultStateKeySerializationStrategy,
  LedgerStateOperations,
  LogAppendingCommitStrategy,
  StateKeySerializationStrategy,
}
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

object BatchedSubmissionValidatorFactory {
  def defaultParametersFor(enableBatching: Boolean): BatchedSubmissionValidatorParameters =
    if (enableBatching) {
      BatchedSubmissionValidatorParameters.reasonableDefault
    } else {
      BatchedSubmissionValidatorParameters(
        cpuParallelism = 1,
        readParallelism = 1,
      )
    }

  class LedgerStateReaderAdapter[LogResult](delegate: LedgerStateOperations[LogResult])
      extends LedgerStateReader {
    override def read(
        keys: Iterable[Raw.StateKey]
    )(implicit
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[Seq[Option[Raw.Envelope]]] =
      delegate.readState(keys)
  }

  def readerAndCommitStrategyFrom[LogResult](
      ledgerStateOperations: LedgerStateOperations[LogResult],
      keySerializationStrategy: StateKeySerializationStrategy = DefaultStateKeySerializationStrategy,
  )(implicit
      executionContext: ExecutionContext
  ): (DamlLedgerStateReader, CommitStrategy[LogResult]) = {
    val ledgerStateReader = DamlLedgerStateReader.from(
      new LedgerStateReaderAdapter[LogResult](ledgerStateOperations),
      keySerializationStrategy,
    )
    val commitStrategy = new LogAppendingCommitStrategy[LogResult](
      ledgerStateOperations,
      keySerializationStrategy,
    )
    (ledgerStateReader, commitStrategy)
  }

  def cachingReaderAndCommitStrategyFrom[LogResult](
      ledgerStateOperations: LedgerStateOperations[LogResult],
      stateCache: Cache[DamlStateKey, DamlStateValue],
      cacheUpdatePolicy: CacheUpdatePolicy[DamlStateKey],
      keySerializationStrategy: StateKeySerializationStrategy = DefaultStateKeySerializationStrategy,
  )(implicit
      executionContext: ExecutionContext
  ): (DamlLedgerStateReader, CommitStrategy[LogResult]) = {
    val ledgerStateReader = CachingStateReader(
      stateCache,
      cacheUpdatePolicy,
      DamlLedgerStateReader.from(
        new LedgerStateReaderAdapter[LogResult](ledgerStateOperations),
        keySerializationStrategy,
      ),
    )
    val commitStrategy = CachingCommitStrategy(
      stateCache,
      cacheUpdatePolicy,
      ledgerStateOperations,
      keySerializationStrategy,
    )
    (ledgerStateReader, commitStrategy)
  }
}
