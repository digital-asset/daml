// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.daml.ledger.api.v1.testing.time_service.*
import com.daml.timer.Timeout.*
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.domain.{LedgerId, optionalLedgerId}
import com.digitalasset.canton.ledger.api.grpc.{GrpcApiService, StreamingServiceLifecycleManagement}
import com.digitalasset.canton.ledger.api.util.TimestampConversion.*
import com.digitalasset.canton.ledger.api.validation.FieldValidator
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.TimeServiceBackend
import com.digitalasset.canton.platform.pekkostreams.dispatcher.SignalDispatcher
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.empty.Empty
import io.grpc.stub.StreamObserver
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import scalaz.syntax.tag.*

import java.time.Instant
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

private[apiserver] final class ApiTimeService private (
    val ledgerId: LedgerId,
    backend: TimeServiceBackend,
    apiStreamShutdownTimeout: Duration,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    mat: Materializer,
    esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
) extends TimeServiceGrpc.TimeService
    with StreamingServiceLifecycleManagement
    with GrpcApiService
    with NamedLogging {

  private val dispatcher = SignalDispatcher[Instant]()

  import FieldValidator.*

  logger.debug(
    s"${getClass.getSimpleName} initialized with ledger ID ${ledgerId.unwrap}, start time ${backend.getCurrentTime}."
  )(TraceContext.empty)

  def getTime(
      request: GetTimeRequest,
      responseObserver: StreamObserver[GetTimeResponse],
  ): Unit = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory, telemetry)
    registerStream(responseObserver) {

      val validated =
        matchLedgerId(ledgerId)(optionalLedgerId(request.ledgerId))
      validated.fold(
        t => Source.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        { ledgerId =>
          logger.info(
            s"Received request for time with ledger ID ${ledgerId.getOrElse("<empty-ledger-id>")}"
          )
          dispatcher
            .subscribe()
            .map(_ => backend.getCurrentTime)
            .scan[Option[Instant]](Some(backend.getCurrentTime)) {
              case (Some(previousTime), currentTime) if previousTime == currentTime => None
              case (_, currentTime) => Some(currentTime)
            }
            .mapConcat {
              case None => Nil
              case Some(t) => List(GetTimeResponse(Some(fromInstant(t))))
            }
            .via(logger.logErrorsOnStream)
        },
      )
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def setTime(request: SetTimeRequest): Future[Empty] = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory, telemetry)

    def updateTime(
        expectedTime: Instant,
        requestedTime: Instant,
    ): Future[Either[StatusRuntimeException, Instant]] = {
      logger.info(s"Setting time to $requestedTime")
      backend
        .setCurrentTime(expectedTime, requestedTime)
        .map(success =>
          if (success) Right(requestedTime)
          else
            Left(
              invalidArgument(
                s"current_time mismatch. Provided: $expectedTime. Actual: ${backend.getCurrentTime}"
              )
            )
        )
    }

    val validatedInput: Either[StatusRuntimeException, (Instant, Instant)] = for {
      _ <- matchLedgerId(ledgerId)(optionalLedgerId(request.ledgerId))
      expectedTime <- FieldValidator
        .requirePresence(request.currentTime, "current_time")
        .map(toInstant)
      requestedTime <- requirePresence(request.newTime, "new_time").map(toInstant)
      _ <- {
        if (!requestedTime.isBefore(expectedTime))
          Right(())
        else
          Left(
            invalidArgument(
              s"new_time [$requestedTime] is before current_time [$expectedTime]. Setting time backwards is not allowed."
            )
          )
      }
    } yield (expectedTime, requestedTime)
    val result: Future[Either[StatusRuntimeException, Empty]] = validatedInput match {
      case Left(err) => Future.successful(Left(err))
      case Right((expectedTime, requestedTime)) =>
        updateTime(expectedTime, requestedTime) map (_.map { _ =>
          dispatcher.signal()
          Empty()
        })
    }

    result
      .andThen(logger.logErrorsOnCall)
      .transform(_.flatMap {
        case Left(error) =>
          logger.warn(s"Failed to set time for request $request: ${error.getMessage}")
          Failure(error)
        case Right(r) => Success(r)
      })
  }

  override def bindService(): ServerServiceDefinition =
    TimeServiceGrpc.bindService(this, executionContext)

  def getCurrentTime: Instant = backend.getCurrentTime

  override def close(): Unit = {
    super.close()
    Await.result(
      dispatcher
        .shutdown()
        .withTimeout(apiStreamShutdownTimeout)(
          logger.warn(
            s"Shutdown of TimeService API streams did not finish in ${apiStreamShutdownTimeout.toSeconds} seconds. System shutdown continues."
          )(TraceContext.empty)
        ),
      Duration.Inf, // .withTimeout above will make sure it completes in apiStreamShutdownTimeout already
    )
  }
}

private[apiserver] object ApiTimeService {
  def create(
      ledgerId: LedgerId,
      backend: TimeServiceBackend,
      apiStreamShutdownTimeout: Duration,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      mat: Materializer,
      esf: ExecutionSequencerFactory,
      executionContext: ExecutionContext,
  ): TimeService with GrpcApiService =
    new ApiTimeService(ledgerId, backend, apiStreamShutdownTimeout, telemetry, loggerFactory)
}
