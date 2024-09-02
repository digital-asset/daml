// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.data.{DeduplicationPeriod, Offset}
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.platform.indexer.parallel.PostPublishData
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Mocked command submission deduplicator that never flags a duplication. */
class NoCommandDeduplicator extends CommandDeduplicator {

  override def processPublications(
      publications: Seq[PostPublishData]
  )(implicit traceContext: TraceContext): Future[Unit] = Future.unit

  /** Always returns an offset and never flags a duplication. */
  override def checkDuplication(
      changeIdHash: ChangeIdHash,
      deduplicationPeriod: DeduplicationPeriod,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DeduplicationFailed, DeduplicationPeriod.DeduplicationOffset] =
    deduplicationPeriod match {
      case offset: DeduplicationPeriod.DeduplicationOffset =>
        EitherT(Future.successful(Either.right(offset)))
      case _: DeduplicationPeriod.DeduplicationDuration =>
        val offset = Offset.firstOffset
        EitherT(Future.successful(Either.right(DeduplicationPeriod.DeduplicationOffset(offset))))
    }
}
