// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.time.Instant

import akka.stream.Materializer
import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.validator.batch.{
  BatchedSubmissionValidator,
  BatchedSubmissionValidatorFactory
}
import com.daml.ledger.validator.caching.{CacheUpdatePolicy, ImmutablesOnlyCacheUpdatePolicy}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Orchestrates committing to a ledger after validating submissions. Supports parallel validation.
  * Example usage, assuming a [[com.daml.ledger.participant.state.kvutils.api.BatchingLedgerWriter]] sends the
  * batched submissions over the wire:
  * {{{
  *   ...
  *   private val ledgerStateOperations = ...
  *   private val validator = BatchedSubmissionValidator.create(ledgerStateOperations)
  *   private val validatingCommitter = BatchedValidatingCommitter(
  *       () => Instant.now(),
  *       validator)
  *   ...
  *
  *   def commitRequestHandler(request: CommitRequest): Future[CommitResponse] =
  *     validatingCommitter.commit(
  *         request.correlationId,
  *         request.envelope,
  *         request.participantId,
  *         ledgerStateOperations)
  *       .map(...)
  * }}}
  *
  * If caching is enabled (i.e., [[stateValueCache]] is not a [[Cache.none]]) then for each request
  * we cache the read state from the ledger and update the cache with the committed state.
  *
  * @param now resolves the current time when processing submission
  * @param keySerializationStrategy strategy for serializing & namespacing state keys
  * @param validator performs actual validation
  * @param stateValueCache cache to be used when reading from and committing to the ledger
  * @tparam LogResult  type of the offset used for a log entry
  */
class BatchedValidatingCommitter[LogResult](
    now: () => Instant,
    keySerializationStrategy: StateKeySerializationStrategy,
    validator: BatchedSubmissionValidator[LogResult],
    stateValueCache: Cache[DamlStateKey, DamlStateValue],
    cacheUpdatePolicy: CacheUpdatePolicy
)(implicit materializer: Materializer) {
  def commit(
      correlationId: String,
      envelope: Bytes,
      submittingParticipantId: ParticipantId,
      ledgerStateOperations: LedgerStateOperations[LogResult]
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult] = {
    val (ledgerStateReader, commitStrategy) = readerAndCommitStrategyFrom(ledgerStateOperations)
    validator
      .validateAndCommit(
        envelope,
        correlationId,
        now(),
        submittingParticipantId,
        ledgerStateReader,
        commitStrategy
      )
      .transformWith {
        case Success(_) =>
          Future.successful(SubmissionResult.Acknowledged)
        case Failure(exception) =>
          Future.successful(SubmissionResult.InternalError(exception.getLocalizedMessage))
      }
  }

  private def readerAndCommitStrategyFrom(ledgerStateOperations: LedgerStateOperations[LogResult])(
      implicit executionContext: ExecutionContext)
    : (DamlLedgerStateReader, CommitStrategy[LogResult]) =
    if (stateValueCache == Cache.none) {
      BatchedSubmissionValidatorFactory
        .readerAndCommitStrategyFrom(ledgerStateOperations, keySerializationStrategy)
    } else {
      BatchedSubmissionValidatorFactory
        .cachingReaderAndCommitStrategyFrom(
          ledgerStateOperations,
          stateValueCache,
          cacheUpdatePolicy,
          keySerializationStrategy)
    }
}

object BatchedValidatingCommitter {
  def apply[LogResult](now: () => Instant, validator: BatchedSubmissionValidator[LogResult])(
      implicit materializer: Materializer): BatchedValidatingCommitter[LogResult] =
    new BatchedValidatingCommitter[LogResult](
      now,
      DefaultStateKeySerializationStrategy,
      validator,
      Cache.none,
      ImmutablesOnlyCacheUpdatePolicy)

  def apply[LogResult](
      now: () => Instant,
      validator: BatchedSubmissionValidator[LogResult],
      stateValueCache: Cache[DamlStateKey, DamlStateValue])(
      implicit materializer: Materializer): BatchedValidatingCommitter[LogResult] =
    new BatchedValidatingCommitter[LogResult](
      now,
      DefaultStateKeySerializationStrategy,
      validator,
      stateValueCache,
      ImmutablesOnlyCacheUpdatePolicy)
}
