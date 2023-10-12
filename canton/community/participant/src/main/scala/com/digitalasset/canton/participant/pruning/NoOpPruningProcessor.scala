// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.Pruning.{
  LedgerPruningError,
  LedgerPruningOnlySupportedInEnterpriseEdition,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait PruningProcessor extends AutoCloseable {
  def pruneLedgerEvents(pruneUpToInclusive: GlobalOffset)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit]

  def safeToPrune(beforeOrAt: CantonTimestamp, boundInclusive: GlobalOffset)(implicit
      traceContext: TraceContext
  ): EitherT[Future, LedgerPruningError, Option[GlobalOffset]]
}

object NoOpPruningProcessor extends PruningProcessor {
  override def pruneLedgerEvents(pruneUpToInclusive: GlobalOffset)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] =
    NoOpPruningProcessor.pruningNotSupportedETUS

  override def safeToPrune(beforeOrAt: CantonTimestamp, boundInclusive: GlobalOffset)(implicit
      traceContext: TraceContext
  ): EitherT[Future, LedgerPruningError, Option[GlobalOffset]] =
    NoOpPruningProcessor.pruningNotSupportedET

  override def close(): Unit = ()

  private val pruningNotSupported: LedgerPruningError =
    LedgerPruningOnlySupportedInEnterpriseEdition
  private def pruningNotSupportedET[A]: EitherT[Future, LedgerPruningError, A] =
    EitherT(Future.successful(Either.left(pruningNotSupported)))

  private def pruningNotSupportedETUS[A]: EitherT[FutureUnlessShutdown, LedgerPruningError, A] =
    EitherT(FutureUnlessShutdown.pure(Either.left(pruningNotSupported)))
}
