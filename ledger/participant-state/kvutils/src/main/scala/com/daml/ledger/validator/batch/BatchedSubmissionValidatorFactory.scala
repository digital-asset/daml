// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator._
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics

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
      keySerializationStrategy: StateKeySerializationStrategy = DefaultStateKeySerializationStrategy)(
      implicit executionContext: ExecutionContext)
    : (DamlLedgerStateReader with QueryableReadSet, CommitStrategy[LogResult]) = {
    val ledgerStateReader = new CachingDamlLedgerStateReader(
      stateCache,
      keySerializationStrategy,
      DamlLedgerStateReader.from(
        new LedgerStateReaderAdapter[LogResult](ledgerStateOperations),
        keySerializationStrategy)
    )
    val commitStrategy = new CachingCommitStrategy(
      stateCache,
      new LogAppendingCommitStrategy[LogResult](ledgerStateOperations, keySerializationStrategy))
    (ledgerStateReader, commitStrategy)
  }

  case class WriteThroughCachingComponents[LogResult](
      ledgerStateReader: DamlLedgerStateReader with QueryableReadSet,
      commitStrategy: CommitStrategy[LogResult],
      batchValidator: BatchedSubmissionValidator[LogResult])

  def componentsForWriteThroughCaching[LogResult](
      params: BatchedSubmissionValidatorParameters,
      ledgerStateOperations: LedgerStateOperations[LogResult],
      stateCache: Cache[DamlStateKey, DamlStateValue],
      metrics: Metrics,
      engine: Engine
  )(implicit executionContext: ExecutionContext): WriteThroughCachingComponents[LogResult] = {
    val (ledgerStateReader, commitStrategy) =
      cachingReaderAndCommitStrategyFrom(ledgerStateOperations, stateCache)
    val batchValidator = BatchedSubmissionValidator[LogResult](
      params,
      engine,
      metrics
    )
    WriteThroughCachingComponents(ledgerStateReader, commitStrategy, batchValidator)
  }
}
