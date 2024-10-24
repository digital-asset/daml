// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.data.{DeduplicationPeriod, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.platform.indexer.parallel.PostPublishData
import com.digitalasset.canton.tracing.TraceContext

/** Mocked command submission deduplicator that never flags a duplication. */
class NoCommandDeduplicator extends CommandDeduplicator {

  override def processPublications(
      publications: Seq[PostPublishData]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

  /** Always returns an offset and never flags a duplication. */
  override def checkDuplication(
      changeIdHash: ChangeIdHash,
      deduplicationPeriod: DeduplicationPeriod,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DeduplicationFailed, DeduplicationPeriod.DeduplicationOffset] =
    deduplicationPeriod match {
      case offset: DeduplicationPeriod.DeduplicationOffset =>
        EitherT(FutureUnlessShutdown.pure(Either.right(offset)))
      case _: DeduplicationPeriod.DeduplicationDuration =>
        val offset = Offset.firstOffset
        EitherT(
          FutureUnlessShutdown.pure(Either.right(DeduplicationPeriod.DeduplicationOffset(offset)))
        )
    }
}
