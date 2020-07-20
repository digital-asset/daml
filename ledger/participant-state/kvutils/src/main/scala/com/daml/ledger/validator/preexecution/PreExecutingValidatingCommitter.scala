// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

import scala.concurrent.duration._
import akka.stream.Materializer
import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{Bytes, Fingerprint}
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.caching.{
  CacheUpdatePolicy,
  CachingDamlLedgerStateReaderWithFingerprints
}
import com.daml.ledger.validator.{
  LedgerStateAccess,
  StateAccessingValidatingCommitter,
  StateKeySerializationStrategy
}
import com.daml.metrics.Metrics
import com.daml.timer.RetryStrategy

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class PreExecutingValidatingCommitter[LogResult](
    now: () => Instant,
    keySerializationStrategy: StateKeySerializationStrategy,
    validator: PreExecutingSubmissionValidator[Seq[(Key, Value)]],
    valueToFingerprint: Option[Value] => Fingerprint,
    postExecutor: PostExecutingStateAccessPersistStrategy[LogResult],
    stateValueCache: Cache[DamlStateKey, (DamlStateValue, Fingerprint)],
    cacheUpdatePolicy: CacheUpdatePolicy,
    metrics: Metrics)(implicit materializer: Materializer)
    extends StateAccessingValidatingCommitter[LogResult] {

  override def commit(
      correlationId: String,
      submissionEnvelope: Bytes,
      submittingParticipantId: ParticipantId,
      ledgerStateAccess: LedgerStateAccess[LogResult])(
      implicit executionContext: ExecutionContext): Future[SubmissionResult] =
    for {
      preExecutionOutput <- validator
        .validate(
          submissionEnvelope,
          correlationId,
          submittingParticipantId,
          CachingDamlLedgerStateReaderWithFingerprints(
            stateValueCache,
            cacheUpdatePolicy,
            new LedgerStateAccessReaderWithFingerprints(ledgerStateAccess, valueToFingerprint),
            keySerializationStrategy,
          )
        )
      submissionResult <- Retry {
        case PostExecutingStateAccessPersistStrategy.Conflict => true
      } { (_, _) =>
        postExecutor.conflictDetectAndPersist(preExecutionOutput, ledgerStateAccess)
      }.transform {
        case result @ Success(_) => result
        case Failure(PostExecutingStateAccessPersistStrategy.Conflict) =>
          Success(SubmissionResult.InternalError("conflict")) // TODO Figure out what's the correct return
        case Failure(exception: Exception) =>
          Success(SubmissionResult.InternalError(exception.getMessage))
      }
    } yield submissionResult

  private[this] val Retry = RetryStrategy.constant(attempts = Some(3), 5.seconds)
}
