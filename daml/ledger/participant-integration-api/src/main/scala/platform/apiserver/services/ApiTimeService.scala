// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.api.util.TimestampConversion._
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.{LedgerId, optionalLedgerId}
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.daml.ledger.api.v1.testing.time_service._
import com.daml.ledger.api.validation.ValidationErrors.invalidArgument
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.akkastreams.dispatcher.SignalDispatcher
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.TimeServiceBackend
import com.daml.platform.server.api.ValidationLogger
import com.daml.platform.server.api.validation.FieldValidations
import com.google.protobuf.empty.Empty
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
import scalaz.syntax.tag._

import scala.concurrent.{Await, ExecutionContext, Future}
import com.daml.timer.Timeout._

import scala.concurrent.duration.Duration

private[apiserver] final class ApiTimeService private (
    val ledgerId: LedgerId,
    backend: TimeServiceBackend,
    apiStreamShutdownTimeout: Duration,
)(implicit
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends TimeServiceAkkaGrpc
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private val dispatcher = SignalDispatcher[Instant]()

  import FieldValidations._

  logger.debug(
    s"${getClass.getSimpleName} initialized with ledger ID ${ledgerId.unwrap}, start time ${backend.getCurrentTime}"
  )

  override protected def getTimeSource(
      request: GetTimeRequest
  ): Source[GetTimeResponse, NotUsed] = {
    val validated =
      matchLedgerId(ledgerId)(optionalLedgerId(request.ledgerId))
    validated.fold(
      t => Source.failed(ValidationLogger.logFailure(request, t)),
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

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def setTime(request: SetTimeRequest): Future[Empty] = {
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

    val result = for {
      _ <- matchLedgerId(ledgerId)(optionalLedgerId(request.ledgerId))
      expectedTime <- FieldValidations
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
    } yield {
      updateTime(expectedTime, requestedTime)
        .map { _ =>
          dispatcher.signal()
          Empty()
        }
        .andThen(logger.logErrorsOnCall[Empty])
    }

    result.fold(
      { error =>
        logger.warn(s"Failed to set time for request $request: ${error.getMessage}")
        Future.failed(error)
      },
      identity,
    )
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
          )
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
  )(implicit
      mat: Materializer,
      esf: ExecutionSequencerFactory,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): TimeService with GrpcApiService =
    new ApiTimeService(ledgerId, backend, apiStreamShutdownTimeout)
}
