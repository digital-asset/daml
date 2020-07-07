// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.export.LedgerDataExporter
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.caching.{
  CacheUpdatePolicy,
  CachingCommitStrategy,
  CachingDamlLedgerStateReader,
  QueryableReadSet
}
import com.daml.ledger.validator.{
  CommitStrategy,
  DamlLedgerStateReader,
  DefaultStateKeySerializationStrategy,
  LedgerStateOperations,
  LedgerStateReader,
  LogAppendingCommitStrategy,
  StateKeySerializationStrategy
}
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics

import scala.concurrent.{ExecutionContext, Future}

object BatchedSubmissionValidatorFactory {
  def defaultParametersFor(enableBatching: Boolean): BatchedSubmissionValidatorParameters =
    if (enableBatching) {
      BatchedSubmissionValidatorParameters.reasonableDefault
    } else {
      BatchedSubmissionValidatorParameters(
        cpuParallelism = 1,
        readParallelism = 1,
        commitParallelism = 1
      )
    }

  class LedgerStateReaderAdapter[LogResult](delegate: LedgerStateOperations[LogResult])
      extends LedgerStateReader {
    override def read(keys: Seq[Key]): Future[Seq[Option[Value]]] = delegate.readState(keys)
  }

  def readerAndCommitStrategyFrom[LogResult](
      ledgerStateOperations: LedgerStateOperations[LogResult],
      keySerializationStrategy: StateKeySerializationStrategy = DefaultStateKeySerializationStrategy,
      ledgerDataExporter: LedgerDataExporter = LedgerDataExporter())(
      implicit executionContext: ExecutionContext)
    : (DamlLedgerStateReader, CommitStrategy[LogResult]) = {
    val ledgerStateReader = DamlLedgerStateReader.from(
      new LedgerStateReaderAdapter[LogResult](ledgerStateOperations),
      keySerializationStrategy)
    val commitStrategy =
      new LogAppendingCommitStrategy[LogResult](
        ledgerStateOperations,
        keySerializationStrategy,
        ledgerDataExporter)
    (ledgerStateReader, commitStrategy)
  }

  def cachingReaderAndCommitStrategyFrom[LogResult](
      ledgerStateOperations: LedgerStateOperations[LogResult],
      stateCache: Cache[DamlStateKey, DamlStateValue],
      cacheUpdatePolicy: CacheUpdatePolicy,
      keySerializationStrategy: StateKeySerializationStrategy = DefaultStateKeySerializationStrategy,
      ledgerDataExporter: LedgerDataExporter = LedgerDataExporter())(
      implicit executionContext: ExecutionContext)
    : (DamlLedgerStateReader with QueryableReadSet, CommitStrategy[LogResult]) = {
    val ledgerStateReader = new CachingDamlLedgerStateReader(
      stateCache,
      cacheUpdatePolicy.shouldCacheOnRead,
      keySerializationStrategy,
      DamlLedgerStateReader.from(
        new LedgerStateReaderAdapter[LogResult](ledgerStateOperations),
        keySerializationStrategy)
    )
    val commitStrategy = new CachingCommitStrategy(
      stateCache,
      cacheUpdatePolicy.shouldCacheOnWrite,
      new LogAppendingCommitStrategy[LogResult](
        ledgerStateOperations,
        keySerializationStrategy,
        ledgerDataExporter)
    )
    (ledgerStateReader, commitStrategy)
  }

  case class CachingEnabledComponents[LogResult](
      ledgerStateReader: DamlLedgerStateReader with QueryableReadSet,
      commitStrategy: CommitStrategy[LogResult],
      batchValidator: BatchedSubmissionValidator[LogResult])

  def componentsEnabledForCaching[LogResult](
      params: BatchedSubmissionValidatorParameters,
      ledgerStateOperations: LedgerStateOperations[LogResult],
      stateCache: Cache[DamlStateKey, DamlStateValue],
      cacheUpdatePolicy: CacheUpdatePolicy,
      metrics: Metrics,
      engine: Engine
  )(implicit executionContext: ExecutionContext): CachingEnabledComponents[LogResult] = {
    val (ledgerStateReader, commitStrategy) =
      cachingReaderAndCommitStrategyFrom(ledgerStateOperations, stateCache, cacheUpdatePolicy)
    val batchValidator = BatchedSubmissionValidator[LogResult](
      params,
      engine,
      metrics
    )
    CachingEnabledComponents(ledgerStateReader, commitStrategy, batchValidator)
  }
}
