// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.platform.apiserver.services.admin.SynchronousResponse.{Accepted, Rejected}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.telemetry.TelemetryContext
import io.grpc.StatusRuntimeException

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, TimeoutException}

/** Submits a request and waits for a corresponding entry to emerge on the corresponding stream.
  *
  * The strategy governs how the request is submitted, how to open a stream, and how to filter for
  * the appropriate entry.
  */
class SynchronousResponse[Input, Entry, AcceptedEntry](
    strategy: SynchronousResponse.Strategy[Input, Entry, AcceptedEntry],
    timeToLive: Duration,
) {

  def submitAndWait(submissionId: Ref.SubmissionId, input: Input)(implicit
      telemetryContext: TelemetryContext,
      executionContext: ExecutionContext,
      materializer: Materializer,
  ): Future[AcceptedEntry] = {
    import state.SubmissionResult
    for {
      ledgerEndBeforeRequest <- strategy.currentLedgerEnd()
      submissionResult <- strategy.submit(submissionId, input)
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
            .completionTimeout(FiniteDuration(timeToLive.toMillis, TimeUnit.MILLISECONDS))
            .runWith(Sink.head)
            .recoverWith { case _: TimeoutException =>
              Future.failed(
                ErrorFactories.aborted("Request timed out", definiteAnswer = Some(false))
              )
            }
            .flatten
        case r: SubmissionResult.SynchronousError =>
          Future.failed(r.exception)
      }
    } yield entry
  }

}

object SynchronousResponse {

  trait Strategy[Input, Entry, AcceptedEntry] {

    /** Fetches the current ledger end before the request is submitted. */
    def currentLedgerEnd(): Future[Option[LedgerOffset.Absolute]]

    /** Submits a request to the ledger. */
    def submit(submissionId: Ref.SubmissionId, input: Input)(implicit
        telemetryContext: TelemetryContext
    ): Future[state.SubmissionResult]

    /** Opens a stream of entries from before the submission. */
    def entries(offset: Option[LedgerOffset.Absolute]): Source[Entry, _]

    /** Filters the entry stream for accepted submissions. */
    def accept(submissionId: Ref.SubmissionId): PartialFunction[Entry, AcceptedEntry]

    /** Filters the entry stream for rejected submissions, and transforms them into appropriate
      * exceptions.
      */
    def reject(submissionId: Ref.SubmissionId): PartialFunction[Entry, StatusRuntimeException]

  }

  private final class Accepted[Entry, AcceptedEntry](
      accept: PartialFunction[Entry, AcceptedEntry]
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
