// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.CommonErrors
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.apiserver.services.admin.SynchronousResponse.{Accepted, Rejected}
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
    timeToLive: FiniteDuration,
)(implicit
    executionContext: ExecutionContext,
    materializer: Materializer,
) {

  private val logger = ContextualizedLogger.get(getClass)

  def submitAndWait(submissionId: Ref.SubmissionId, input: Input)(implicit
      telemetryContext: TelemetryContext,
      loggingContext: LoggingContext,
  ): Future[AcceptedEntry] = {
    for {
      ledgerEndBeforeRequest <- strategy.currentLedgerEnd()
      submissionResult <- strategy.submit(submissionId, input)
      entry <- toResult(submissionId, ledgerEndBeforeRequest, submissionResult)
    } yield entry
  }

  private def toResult(
      submissionId: Ref.SubmissionId,
      ledgerEndBeforeRequest: Option[LedgerOffset.Absolute],
      submissionResult: SubmissionResult,
  )(implicit loggingContext: LoggingContext) = submissionResult match {
    case SubmissionResult.Acknowledged =>
      acknowledged(submissionId, ledgerEndBeforeRequest)
    case synchronousError: SubmissionResult.SynchronousError =>
      Future.failed(synchronousError.exception)
  }

  private def acknowledged(
      submissionId: Ref.SubmissionId,
      ledgerEndBeforeRequest: Option[LedgerOffset.Absolute],
  )(implicit loggingContext: LoggingContext) = {
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
      .recoverWith(toGrpcError(loggingContext, submissionId))
      .flatten
  }

  private def toGrpcError(
      loggingContext: LoggingContext,
      submissionId: Ref.SubmissionId,
  ): PartialFunction[Throwable, Future[Nothing]] = {
    case _: TimeoutException =>
      Future.failed(
        CommonErrors.RequestTimeOut
          .Reject("Request timed out", definiteAnswer = false)(
            errorLogger(loggingContext, submissionId)
          )
          .asGrpcError
      )
    case _: NoSuchElementException =>
      Future.failed(
        CommonErrors.ServiceNotRunning
          .Reject("Party submission")(errorLogger(loggingContext, submissionId))
          .asGrpcError
      )
  }

  private def errorLogger(loggingContext: LoggingContext, submissionId: Ref.SubmissionId) =
    new DamlContextualizedErrorLogger(logger, loggingContext, Some(submissionId))

}

object SynchronousResponse {

  trait Strategy[Input, Entry, AcceptedEntry] {

    /** Fetches the current ledger end before the request is submitted. */
    def currentLedgerEnd(): Future[Option[LedgerOffset.Absolute]]

    /** Submits a request to the ledger. */
    def submit(submissionId: Ref.SubmissionId, input: Input)(implicit
        telemetryContext: TelemetryContext,
        loggingContext: LoggingContext,
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
