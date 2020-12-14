// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{Bytes, Fingerprint}
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.caching.{
  CacheUpdatePolicy,
  CachingDamlLedgerStateReaderWithFingerprints
}
import com.daml.ledger.validator.preexecution.PreExecutionCommitResult.ReadSet
import com.daml.ledger.validator.{
  LedgerStateAccess,
  LedgerStateOperationsReaderAdapter,
  StateKeySerializationStrategy
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.timer.RetryStrategy

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * A pre-executing validating committer based on [[LedgerStateAccess]] (that does not provide
  * fingerprints alongside values), parametric in the logic that produces a fingerprint given a
  * value.
  *
  * @param keySerializationStrategy      The key serializer used for state keys.
  * @param validator                     The pre-execution validator.
  * @param valueToFingerprint            The logic that produces a fingerprint given a value.
  * @param postExecutionConflictDetector The post-execution conflict detector.
  * @param postExecutionFinalizer        The post-execution finalizer.
  * @param stateValueCache               The cache instance for state values.
  * @param cacheUpdatePolicy             The caching policy for values.
  */
class PreExecutingValidatingCommitter[WriteSet](
    keySerializationStrategy: StateKeySerializationStrategy,
    validator: PreExecutingSubmissionValidator[ReadSet, WriteSet],
    valueToFingerprint: Option[Value] => Fingerprint,
    postExecutionConflictDetector: PostExecutionConflictDetector[
      Key,
      (Option[Value], Fingerprint),
      ReadSet,
      WriteSet,
    ],
    postExecutionFinalizer: PostExecutionFinalizer[ReadSet, WriteSet],
    stateValueCache: Cache[DamlStateKey, (DamlStateValue, Fingerprint)],
    cacheUpdatePolicy: CacheUpdatePolicy[DamlStateKey],
) {

  private val logger = ContextualizedLogger.get(getClass)

  /**
    * Pre-executes and then commits a submission.
    */
  def commit[LogResult](
      correlationId: String,
      submissionEnvelope: Bytes,
      submittingParticipantId: ParticipantId,
      ledgerStateAccess: LedgerStateAccess[LogResult],
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult] =
    LoggingContext.newLoggingContext("correlationId" -> correlationId) { implicit loggingContext =>
      // Sequential pre-execution, implemented by enclosing the whole pre-post-exec pipeline is a single transaction.
      ledgerStateAccess.inTransaction { ledgerStateOperations =>
        val stateReader = new LedgerStateOperationsReaderAdapter(ledgerStateOperations)
          .mapValues(value => value -> valueToFingerprint(value))
        val cachingStateReader = CachingDamlLedgerStateReaderWithFingerprints(
          stateValueCache,
          cacheUpdatePolicy,
          stateReader,
          keySerializationStrategy,
        )
        for {
          preExecutionOutput <- validator.validate(
            submissionEnvelope,
            submittingParticipantId,
            cachingStateReader,
          )
          _ <- retry {
            case _: ConflictDetectedException =>
              logger.error("Conflict detected during post-execution. Retrying...")
              true
          } { (_, _) =>
            postExecutionConflictDetector.detectConflicts(preExecutionOutput, stateReader)
          }.transform {
            case Failure(_: ConflictDetectedException) =>
              logger.error("Too many conflicts detected during post-execution. Giving up.")
              Success(SubmissionResult.Acknowledged) // But it will simply be dropped.
            case result => result
          }
          submissionResult <- postExecutionFinalizer.finalizeSubmission(
            preExecutionOutput,
            ledgerStateOperations)
        } yield submissionResult
      }
    }

  private[this] def retry: PartialFunction[Throwable, Boolean] => RetryStrategy =
    RetryStrategy.constant(attempts = Some(3), 5.seconds)
}
