// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.digitalasset.canton.ledger.api.domain.ParticipantOffset
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.SubmissionResult
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.platform.apiserver.services.admin.SynchronousResponse.{
  Accepted,
  Rejected,
}
import com.digitalasset.daml.lf.data.Ref
import io.grpc.StatusRuntimeException
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.{KillSwitches, Materializer}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, TimeoutException}

/** Submits a request and waits for a corresponding entry to emerge on the corresponding stream.
  *
  * The strategy governs how the request is submitted, how to open a stream, and how to filter for
  * the appropriate entry.
  */
class SynchronousResponse[Input, Entry, AcceptedEntry](
    strategy: SynchronousResponse.Strategy[Input, Entry, AcceptedEntry],
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext,
    materializer: Materializer,
) extends AutoCloseable
    with NamedLogging {

  private val shutdownKillSwitch = KillSwitches.shared("shutdown-synchronous-response")

  object ShuttingDown extends RuntimeException;

  override def close(): Unit = shutdownKillSwitch.abort(ShuttingDown)

  def submitAndWait(
      submissionId: Ref.SubmissionId,
      input: Input,
      ledgerEndBeforeRequest: Option[ParticipantOffset.Absolute],
      timeToLive: FiniteDuration,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[AcceptedEntry] = {
    for {
      submissionResult <- strategy.submit(submissionId, input)
      entry <- toResult(submissionId, ledgerEndBeforeRequest, submissionResult, timeToLive)
    } yield entry
  }

  private def toResult(
      submissionId: Ref.SubmissionId,
      ledgerEndBeforeRequest: Option[ParticipantOffset.Absolute],
      submissionResult: SubmissionResult,
      timeToLive: FiniteDuration,
  )(implicit loggingContext: LoggingContextWithTrace) = submissionResult match {
    case SubmissionResult.Acknowledged =>
      acknowledged(submissionId, ledgerEndBeforeRequest, timeToLive)
    case synchronousError: SubmissionResult.SynchronousError =>
      Future.failed(synchronousError.exception)
  }

  private def acknowledged(
      submissionId: Ref.SubmissionId,
      ledgerEndBeforeRequest: Option[ParticipantOffset.Absolute],
      timeToLive: FiniteDuration,
  )(implicit loggingContext: LoggingContextWithTrace) = {
    val isAccepted = new Accepted(strategy.accept(submissionId))
    val isRejected = new Rejected(strategy.reject(submissionId))
    val contextualizedErrorLogger = {
      ErrorLoggingContext(logger, loggingContext.toPropertiesMap, loggingContext.traceContext)
    }
    strategy
      .entries(ledgerEndBeforeRequest)
      .via(shutdownKillSwitch.flow)
      .mapError { case ShuttingDown =>
        // This is needed for getting a different instance of StatusRuntimeException for each shut down stream
        CommonErrors.ServerIsShuttingDown.Reject()(contextualizedErrorLogger).asGrpcError
      }
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
      loggingContext: LoggingContextWithTrace,
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

  private def errorLogger(loggingContext: LoggingContextWithTrace, submissionId: Ref.SubmissionId) =
    LedgerErrorLoggingContext(
      logger,
      loggingContext.toPropertiesMap,
      loggingContext.traceContext,
      submissionId,
    )

}

object SynchronousResponse {

  trait Strategy[Input, Entry, AcceptedEntry] {

    /** Submits a request to the ledger. */
    def submit(submissionId: Ref.SubmissionId, input: Input)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[state.SubmissionResult]

    /** Opens a stream of entries from before the submission. */
    def entries(offset: Option[ParticipantOffset.Absolute])(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[Entry, ?]

    /** Filters the entry stream for accepted submissions. */
    def accept(submissionId: Ref.SubmissionId): PartialFunction[Entry, AcceptedEntry]

    /** Filters the entry stream for rejected submissions, and transforms them into appropriate
      * exceptions.
      */
    def reject(submissionId: Ref.SubmissionId)(implicit
        loggingContext: LoggingContextWithTrace
    ): PartialFunction[Entry, StatusRuntimeException]

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
