// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.Foldable
import cats.data.EitherT
import cats.implicits.toFoldableOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.pruning.PruningStatus
import com.digitalasset.canton.store.CursorPrehead.RequestCounterCursorPrehead
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.EitherUtil

import scala.concurrent.{ExecutionContext, Future}

private[inspection] object TimestampValidation {

  private def validate[A, F[_]: Foldable](ffa: Future[F[A]])(p: A => Boolean)(fail: A => Error)(
      implicit ec: ExecutionContext
  ): EitherT[Future, Error, Unit] =
    EitherT(ffa.map(_.traverse_(a => EitherUtil.condUnitE(p(a), fail(a)))))

  def beforePrehead(
      domainId: DomainId,
      cursorPrehead: Future[Option[RequestCounterCursorPrehead]],
      timestamp: CantonTimestamp,
  )(implicit ec: ExecutionContext): EitherT[Future, Error, Unit] =
    validate(cursorPrehead)(timestamp < _.timestamp)(cp =>
      Error.TimestampAfterPrehead(domainId, timestamp, cp.timestamp)
    )

  def afterPruning(
      domainId: DomainId,
      pruningStatus: Future[Option[PruningStatus]],
      timestamp: CantonTimestamp,
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, Error, Unit] =
    validate(pruningStatus)(timestamp >= _.timestamp)(ps =>
      Error.TimestampBeforePruning(domainId, timestamp, ps.timestamp)
    )

}
