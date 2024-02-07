// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.admin.pruning.v30
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule, PruningScheduler}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

trait GrpcPruningScheduler {
  this: HasPruningScheduler & NamedLogging =>

  def setSchedule(request: v30.SetSchedule.Request): Future[v30.SetSchedule.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      schedule <- convertRequiredF("schedule", request.schedule, PruningSchedule.fromProtoV30)
      _scheduleSuccessfullySet <- handlePassiveHAStorageError(
        scheduler.setSchedule(schedule),
        "set_schedule",
      )
    } yield v30.SetSchedule.Response()
  }

  def clearSchedule(
      request: v30.ClearSchedule.Request
  ): Future[v30.ClearSchedule.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      _ <- handlePassiveHAStorageError(scheduler.clearSchedule(), "clear_schedule")
    } yield v30.ClearSchedule.Response()
  }

  def setCron(request: v30.SetCron.Request): Future[v30.SetCron.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      cron <- convertF(Cron.fromProtoPrimitive(request.cron))
      _cronSuccessfullySet <- handlePassiveHAStorageError(
        handleUserError(scheduler.updateCron(cron)),
        "set_cron",
      )
    } yield v30.SetCron.Response()
  }

  def setMaxDuration(
      request: v30.SetMaxDuration.Request
  ): Future[v30.SetMaxDuration.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      positiveDuration <- convertF(
        PositiveSeconds
          .fromProtoPrimitiveO("max_duration")(request.maxDuration)
      )
      _maxDurationSuccessfullySet <- handlePassiveHAStorageError(
        handleUserError(scheduler.updateMaxDuration(positiveDuration)),
        "set_max_duration",
      )
    } yield v30.SetMaxDuration.Response()
  }

  def setRetention(
      request: v30.SetRetention.Request
  ): Future[v30.SetRetention.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      positiveDuration <- convertF(
        PositiveSeconds
          .fromProtoPrimitiveO("retention")(request.retention)
      )
      _retentionSuccessfullySet <- handlePassiveHAStorageError(
        handleUserError(scheduler.updateRetention(positiveDuration)),
        "set_retention",
      )
    } yield v30.SetRetention.Response()
  }

  def getSchedule(
      request: v30.GetSchedule.Request
  ): Future[v30.GetSchedule.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      scheduleWithRetention <- scheduler.getSchedule()
    } yield v30.GetSchedule.Response(scheduleWithRetention.map(_.toProtoV30))
  }

  private def convertF[T](f: => ProtoConverter.ParsingResult[T])(implicit
      traceContext: TraceContext
  ): Future[T] = f
    .leftMap(err => ProtoDeserializationFailure.Wrap(err).asGrpcError)
    .fold(Future.failed, Future.successful)

  protected def convertRequiredF[P, T](
      field: String,
      value: Option[P],
      f: P => ProtoConverter.ParsingResult[T],
  )(implicit traceContext: TraceContext): Future[T] = convertF(
    ProtoConverter.required(field, value).flatMap(f)
  )

  private def handleUserError(update: EitherT[Future, String, Unit]): Future[Unit] =
    EitherTUtil.toFuture(
      update.leftMap(
        Status.INVALID_ARGUMENT
          .withDescription(_)
          .asRuntimeException()
      )
    )

  protected def handlePassiveHAStorageError(
      update: Future[Unit],
      commandName: String,
  ): Future[Unit] =
    update.transform {
      case Failure(PassiveInstanceException(_internalMessage)) =>
        Failure(
          Status.UNAVAILABLE
            .withDescription(
              s"Command ${commandName} sent to passive replica: cannot modify the pruning schedule. Try to submit the command to another replica."
            )
            .asRuntimeException()
        )
      case x => x
    }

}

trait HasPruningScheduler {
  protected def ensureScheduler(implicit
      traceContext: TraceContext
  ): Future[PruningScheduler]

  implicit val ec: ExecutionContext
}
