// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

import akka.stream.Materializer
import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.api.{
  BatchingLedgerWriter,
  BatchingLedgerWriterConfig,
  BatchingQueue,
  DefaultBatchingQueue,
  LedgerWriter
}
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
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future}

object BatchedSubmissionValidatorFactory {
  def defaultParametersFor(enableBatching: Boolean): BatchedSubmissionValidatorParameters =
    if (enableBatching) {
      BatchedSubmissionValidatorParameters.default
    } else {
      BatchedSubmissionValidatorParameters(
        cpuParallelism = 1,
        readParallelism = 1,
        commitParallelism = 1
      )
    }

  def batchingLedgerWriterFrom(
      batchingLedgerWriterConfig: BatchingLedgerWriterConfig,
      delegate: LedgerWriter)(
      implicit materializer: Materializer,
      loggingContext: LoggingContext): BatchingLedgerWriter = {
    val batchingQueue = batchingQueueFrom(batchingLedgerWriterConfig)
    new BatchingLedgerWriter(batchingQueue, delegate)
  }

  private def batchingQueueFrom(
      batchingLedgerWriterConfig: BatchingLedgerWriterConfig): BatchingQueue =
    if (batchingLedgerWriterConfig.enableBatching) {
      DefaultBatchingQueue(
        maxQueueSize = batchingLedgerWriterConfig.maxBatchQueueSize,
        maxBatchSizeBytes = batchingLedgerWriterConfig.maxBatchSizeBytes,
        maxWaitDuration = batchingLedgerWriterConfig.maxBatchWaitDuration,
        maxConcurrentCommits = batchingLedgerWriterConfig.maxBatchConcurrentCommits
      )
    } else {
      batchingQueueForSerialValidation(batchingLedgerWriterConfig.maxBatchQueueSize)
    }

  private def batchingQueueForSerialValidation(maxBatchQueueSize: Int): DefaultBatchingQueue =
    DefaultBatchingQueue(
      maxQueueSize = maxBatchQueueSize,
      maxBatchSizeBytes = 1,
      maxWaitDuration = Duration(1, MILLISECONDS),
      maxConcurrentCommits = 1
    )

  class LedgerStateReaderAdapter[LogResult](delegate: LedgerStateOperations[LogResult])
      extends LedgerStateReader {
    override def read(keys: Seq[Key]): Future[Seq[Option[Value]]] = delegate.readState(keys)
  }

  def readerAndCommitStrategyFrom[LogResult](
      ledgerStateOperations: LedgerStateOperations[LogResult],
      keySerializationStrategy: StateKeySerializationStrategy = DefaultStateKeySerializationStrategy)(
      implicit executionContext: ExecutionContext)
    : (DamlLedgerStateReader, CommitStrategy[LogResult]) = {
    val ledgerStateReader = DamlLedgerStateReader.from(
      new LedgerStateReaderAdapter[LogResult](ledgerStateOperations),
      keySerializationStrategy)
    val commitStrategy =
      new LogAppendingCommitStrategy[LogResult](ledgerStateOperations, keySerializationStrategy)
    (ledgerStateReader, commitStrategy)
  }

  def cachingReaderAndCommitStrategyFrom[LogResult](
      ledgerStateOperations: LedgerStateOperations[LogResult],
      stateCache: Cache[DamlStateKey, DamlStateValue],
      cacheUpdatePolicy: CacheUpdatePolicy,
      keySerializationStrategy: StateKeySerializationStrategy = DefaultStateKeySerializationStrategy)(
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
      new LogAppendingCommitStrategy[LogResult](ledgerStateOperations, keySerializationStrategy))
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
