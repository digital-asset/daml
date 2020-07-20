// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

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

import scala.concurrent.{ExecutionContext, Future}

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
      submissionResult <- postExecutor.conflictDetectAndPersist(
        preExecutionOutput,
        ledgerStateAccess)
    } yield submissionResult
}
