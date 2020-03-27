// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver.services

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.digitalasset.ledger.api.v1.testing.time_service._
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.akkastreams.dispatcher.SignalDispatcher
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.apiserver.TimeServiceBackend
import com.digitalasset.platform.server.api.validation.FieldValidations
import com.google.protobuf.empty.Empty
import io.grpc.{ServerServiceDefinition, Status, StatusRuntimeException}
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace

final class ApiTimeService private (
    val ledgerId: LedgerId,
    backend: TimeServiceBackend,
)(
    implicit grpcExecutionContext: ExecutionContext,
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory,
    logCtx: LoggingContext)
    extends TimeServiceAkkaGrpc
    with FieldValidations
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  logger.debug(
    s"${getClass.getSimpleName} initialized with ledger ID ${ledgerId.unwrap}, start time ${backend.getCurrentTime}")

  private val dispatcher = SignalDispatcher[Instant]()

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override protected def getTimeSource(request: GetTimeRequest): Source[GetTimeResponse, NotUsed] =
    matchLedgerId(ledgerId)(LedgerId(request.ledgerId)).fold(
      Source.failed, { ledgerId =>
        logger.trace(s"Request for time with ledger ID $ledgerId")
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
      }
    )

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def setTime(request: SetTimeRequest): Future[Empty] = {
    def updateTime(
        expectedTime: Instant,
        requestedTime: Instant): Future[Either[StatusRuntimeException, Instant]] =
      backend
        .setCurrentTime(expectedTime, requestedTime)
        .map(success =>
          if (success) Right(requestedTime)
          else
            Left(
              new StatusRuntimeException(Status.INVALID_ARGUMENT
                .withDescription(
                  s"current_time mismatch. Provided: $expectedTime. Actual: ${backend.getCurrentTime}"))
              with NoStackTrace
          ))(DirectExecutionContext)

    val result = for {
      _ <- matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      expectedTime <- requirePresence(request.currentTime, "current_time").map(toInstant)
      requestedTime <- requirePresence(request.newTime, "new_time").map(toInstant)
      _ <- {
        if (!requestedTime.isBefore(expectedTime))
          Right(())
        else
          Left(
            new StatusRuntimeException(Status.INVALID_ARGUMENT
              .withDescription(
                s"new_time [$requestedTime] is before current_time [$expectedTime]. Setting time backwards is not allowed."))
            with NoStackTrace)
      }
    } yield {
      updateTime(expectedTime, requestedTime)
        .map { _ =>
          dispatcher.signal()
          Empty()
        }
        .andThen(logger.logErrorsOnCall[Empty])
    }

    result.fold({ error =>
      logger.warn(s"Failed to set time for request $request: ${error.getMessage}")
      Future.failed(error)
    }, identity)
  }

  override def bindService(): ServerServiceDefinition =
    TimeServiceGrpc.bindService(this, DirectExecutionContext)

  def getCurrentTime: Instant = backend.getCurrentTime

  override def close(): Unit = {
    super.close()
    dispatcher.close()
  }
}

object ApiTimeService {
  def create(ledgerId: LedgerId, backend: TimeServiceBackend)(
      implicit grpcExecutionContext: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory,
      logCtx: LoggingContext): TimeService with GrpcApiService =
    new ApiTimeService(ledgerId, backend)
}
