// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.store
import com.digitalasset.canton.participant.store.MultiDomainEventLog
import com.digitalasset.canton.participant.sync.UpstreamOffsetConvert
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Mocked command submission deduplicator that never flags a duplication. */
class NoCommandDeduplicator extends CommandDeduplicator {

  override def processPublications(
      publications: Seq[store.MultiDomainEventLog.OnPublish.Publication]
  )(implicit traceContext: TraceContext): Future[Unit] = Future.unit

  /** Always returns an offset and never flags a duplication. */
  override def checkDuplication(
      changeIdHash: ChangeIdHash,
      deduplicationPeriod: DeduplicationPeriod,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DeduplicationFailed, DeduplicationPeriod.DeduplicationOffset] = {
    deduplicationPeriod match {
      case offset: DeduplicationPeriod.DeduplicationOffset =>
        EitherT(Future.successful(Either.right(offset)))
      case _: DeduplicationPeriod.DeduplicationDuration =>
        val offset = UpstreamOffsetConvert.fromGlobalOffset(MultiDomainEventLog.ledgerFirstOffset)
        EitherT(Future.successful(Either.right(DeduplicationPeriod.DeduplicationOffset(offset))))
    }
  }
}
