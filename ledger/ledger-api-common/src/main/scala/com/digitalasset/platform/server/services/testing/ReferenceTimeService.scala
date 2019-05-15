// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.testing

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.daml.lf.data.Ref.LedgerId
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.digitalasset.ledger.api.v1.testing.time_service._
import com.digitalasset.platform.akkastreams.dispatcher.SignalDispatcher
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.api.validation.FieldValidations
import com.google.protobuf.empty.Empty
import io.grpc.{BindableService, ServerServiceDefinition, Status, StatusRuntimeException}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace

class ReferenceTimeService private (
    val ledgerId: LedgerId,
    backend: TimeServiceBackend,
    allowSettingTimeBackwards: Boolean
)(
    implicit grpcExecutionContext: ExecutionContext,
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory)
    extends TimeServiceAkkaGrpc
    with FieldValidations
    with GrpcApiService {

  protected val logger = LoggerFactory.getLogger(TimeServiceGrpc.TimeService.getClass)

  logger.debug(
    "{} initialized with ledger ID {}, start time {}",
    this.getClass.getSimpleName,
    ledgerId,
    backend.getCurrentTime)

  private val dispatcher = SignalDispatcher()

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override protected def getTimeSource(
      request: GetTimeRequest): Source[GetTimeResponse, NotUsed] = {

    matchLedgerId(ledgerId)(request.ledgerId).fold(
      Source.failed, { ledgerId =>
        logger.trace("Request for time with ledger ID {}", ledgerId)
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
      }
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def setTime(request: SetTimeRequest): Future[Empty] = {
    @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
    implicit val dec = DirectExecutionContext

    def updateTime(
        expectedTime: Instant,
        requestedTime: Instant): Future[Either[StatusRuntimeException, Instant]] = {
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
          ))
    }

    val result = for {
      _ <- matchLedgerId(ledgerId)(request.ledgerId)
      expectedTime <- requirePresence(request.currentTime, "current_time").map(toInstant)
      requestedTime <- requirePresence(request.newTime, "new_time").map(toInstant)
      _ <- {
        if (allowSettingTimeBackwards || !requestedTime.isBefore(expectedTime)) Right(())
        else
          Left(
            new StatusRuntimeException(Status.INVALID_ARGUMENT
              .withDescription(
                s"new_time [$requestedTime] is before current_time [$expectedTime]. Setting time backwards is not allowed."))
            with NoStackTrace)
      }
      //_ <- updateTime(expectedTime, requestedTime)
    } yield {
      updateTime(expectedTime, requestedTime).map { _ =>
        dispatcher.signal()
        Empty()
      }
    }

    result.fold({ error =>
      logger.warn("Failed to set time for request {}: {}", Array(request, error.getMessage))
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

object ReferenceTimeService {
  def apply(
      ledgerId: LedgerId,
      backend: TimeServiceBackend,
      allowSettingTimeBackwards: Boolean = false)(
      implicit grpcExecutionContext: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory): TimeService with BindableService with TimeServiceLogging =
    new ReferenceTimeService(ledgerId, backend, allowSettingTimeBackwards) with TimeServiceLogging
}
