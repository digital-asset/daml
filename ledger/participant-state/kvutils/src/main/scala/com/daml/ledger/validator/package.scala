// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger

import com.daml.ledger.participant.state.kvutils.{Bytes, CorrelationId}
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}

import scala.concurrent.{ExecutionContext, Future}

package object validator {
  type SubmissionEnvelope = Bytes
  type SubmittingParticipantId = ParticipantId

  /**
    * Orchestrates committing to a ledger after validating submissions.
    */
  type ValidateAndCommit = (
      CorrelationId,
      SubmissionEnvelope,
      SubmittingParticipantId,
  ) => Future[SubmissionResult]

  // At some point, someone much smarter than the author of this code will reimplement the usages of
  // `inParallel` using Cats or Scalaz.
  //
  // Until then, these will do. They're not public because they're not part of the API.

  private[validator] def inParallel[A, B](
      aFuture: Future[A],
      bFuture: Future[B],
  )(implicit executionContext: ExecutionContext): Future[(A, B)] =
    for {
      a <- aFuture
      b <- bFuture
    } yield (a, b)

  private[validator] def inParallel[A, B, C](
      aFuture: Future[A],
      bFuture: Future[B],
      cFuture: Future[C],
  )(implicit executionContext: ExecutionContext): Future[(A, B, C)] =
    for {
      a <- aFuture
      b <- bFuture
      c <- cFuture
    } yield (a, b, c)
}
