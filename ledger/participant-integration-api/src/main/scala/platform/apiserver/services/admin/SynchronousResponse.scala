// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.participant.state.v1.{SubmissionId, SubmissionResult}
import com.daml.platform.apiserver.services.admin.SynchronousResponse.{Accepted, Rejected}
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.StatusRuntimeException

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, TimeoutException}

class SynchronousResponse[Entry, AcceptedEntry](
    timeToLive: FiniteDuration,
    strategy: SynchronousResponse.Strategy[Entry, AcceptedEntry],
) {

  def submitAndWait(submissionId: SubmissionId)(
      implicit executionContext: ExecutionContext,
      materializer: Materializer,
  ): Future[AcceptedEntry] = {
    for {
      ledgerEndBeforeRequest <- strategy.currentLedgerEnd()
      submissionResult <- strategy.submit(submissionId)
      entry <- submissionResult match {
        case SubmissionResult.Acknowledged =>
          val isAccepted = new Accepted(strategy.accept(submissionId))
          val isRejected = new Rejected(strategy.reject(submissionId))
          strategy
            .entries(ledgerEndBeforeRequest)
            .collect {
              case isAccepted(entry) => Future.successful(entry)
              case isRejected(exception) => Future.failed(exception)
            }
            .completionTimeout(timeToLive)
            .runWith(Sink.head)
            .recoverWith {
              case _: TimeoutException =>
                Future.failed(ErrorFactories.aborted("Request timed out"))
            }
            .flatten
        case r @ SubmissionResult.Overloaded =>
          Future.failed(ErrorFactories.resourceExhausted(r.description))
        case r @ SubmissionResult.InternalError(_) =>
          Future.failed(ErrorFactories.internal(r.reason))
        case r @ SubmissionResult.NotSupported =>
          Future.failed(ErrorFactories.unimplemented(r.description))
      }
    } yield entry
  }

}

object SynchronousResponse {

  def pollUntilPersisted[T](
      source: Source[T, _],
      timeToLive: FiniteDuration,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): Future[T] =
    source
      .completionTimeout(timeToLive)
      .runWith(Sink.head)
      .recoverWith {
        case _: TimeoutException =>
          Future.failed(ErrorFactories.aborted("Request timed out"))
      }

  trait Strategy[Entry, AcceptedEntry] {

    def currentLedgerEnd(): Future[Option[LedgerOffset.Absolute]]

    def submit(submissionId: SubmissionId): Future[SubmissionResult]

    def entries(offset: Option[LedgerOffset.Absolute]): Source[Entry, _]

    def accept(submissionId: SubmissionId): PartialFunction[Entry, AcceptedEntry]

    def reject(submissionId: SubmissionId): PartialFunction[Entry, StatusRuntimeException]

  }

  private final class Accepted[Entry, AcceptedEntry](
      accept: PartialFunction[Entry, AcceptedEntry],
  ) {
    private val liftedAccept = accept.lift

    def unapply(entry: Entry): Option[AcceptedEntry] =
      liftedAccept(entry)
  }

  private final class Rejected[Entry](reject: PartialFunction[Entry, StatusRuntimeException]) {
    private val liftedReject = reject.lift

    def unapply(entry: Entry): Option[StatusRuntimeException] =
      liftedReject(entry)
  }

}
