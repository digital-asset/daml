// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

import akka.stream.Materializer
import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{Bytes, Fingerprint}
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.validator.LedgerStateOperations.Value
import com.daml.ledger.validator.SubmissionValidator.RawKeyValuePairs
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

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class PreExecutingValidatingCommitter[LogResult](
    now: () => Instant,
    keySerializationStrategy: StateKeySerializationStrategy,
    validator: PreExecutingSubmissionValidator[RawKeyValuePairs],
    valueToFingerprint: Option[Value] => Fingerprint,
    postExecutor: PostExecutingPersistStrategy[LogResult],
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
    // Fidelity level 1: sequential pre-execution. Implemented as: the pre-post-exec pipeline is a single transaction.
    ledgerStateAccess.inTransaction { ledgerStateOperations =>
      for {
        preExecutionOutput <- validator
          .validate(
            submissionEnvelope,
            correlationId,
            submittingParticipantId,
            CachingDamlLedgerStateReaderWithFingerprints(
              stateValueCache,
              cacheUpdatePolicy,
              new LedgerReaderWithFingerprints(ledgerStateOperations, valueToFingerprint),
              keySerializationStrategy,
            )
          )
        submissionResult <- retry {
          case PostExecutingPersistStrategy.Conflict => true
        } { (_, _) =>
          postExecutor.conflictDetectAndPersist(now, preExecutionOutput, ledgerStateOperations)
        }.transform {
          case Failure(PostExecutingPersistStrategy.Conflict) =>
            Success(SubmissionResult.Acknowledged) // Will simply be dropped
          case result => result
        }
      } yield submissionResult
    }

  private[this] def retry: PartialFunction[Throwable, Boolean] => RetryStrategy =
    RetryStrategy.constant(attempts = Some(3), 5.seconds)
}
