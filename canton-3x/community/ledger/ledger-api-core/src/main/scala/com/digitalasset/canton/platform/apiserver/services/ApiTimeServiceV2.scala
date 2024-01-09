// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v2.testing.time_service.TimeServiceGrpc.TimeService
import com.daml.ledger.api.v2.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  SetTimeRequest,
  TimeServiceGrpc,
}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.util.TimestampConversion.*
import com.digitalasset.canton.ledger.api.validation.FieldValidator
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.TimeServiceBackend
import com.google.protobuf.empty.Empty
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[apiserver] final class ApiTimeServiceV2(
    backend: TimeServiceBackend,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends TimeService
    with GrpcApiService
    with NamedLogging {

  import FieldValidator.*

  def getTime(request: GetTimeRequest): Future[GetTimeResponse] = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory, telemetry)

    logger.info(s"Received request for time.")
    Future.successful(GetTimeResponse(Some(fromInstant(backend.getCurrentTime))))
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

  override def close(): Unit = ()
}
