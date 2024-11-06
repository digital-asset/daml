// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import com.digitalasset.canton.GrpcServiceInvocationMethod
import com.digitalasset.canton.admin.api.client.data.PruningSchedule
import com.digitalasset.canton.admin.pruning.v30
import com.digitalasset.canton.admin.pruning.v30.{PruningSchedule as PruningScheduleP, *}
import com.digitalasset.canton.config.PositiveDurationSeconds
import io.grpc.ManagedChannel
import io.grpc.stub.AbstractStub

import scala.concurrent.Future

/** Exposes shared grpc client pruning scheduler commands reusable by participant/mediator/sequencer
  * admin api.
  * Having to type-parameterize as grpc does not support inheritance and passing in the grpc stub methods in one by one
  */
@GrpcServiceInvocationMethod
class PruningSchedulerCommands[Stub <: AbstractStub[Stub]](
    createServiceStub: ManagedChannel => Stub,
    submitSetSchedule: (Stub, SetSchedule.Request) => Future[SetSchedule.Response],
    submitClearSchedule: (Stub, ClearSchedule.Request) => Future[ClearSchedule.Response],
    submitSetCron: (Stub, SetCron.Request) => Future[SetCron.Response],
    submitSetMaxDuration: (Stub, v30.SetMaxDuration.Request) => Future[SetMaxDuration.Response],
    submitSetRetention: (Stub, SetRetention.Request) => Future[SetRetention.Response],
    submitGetSchedule: (Stub, GetSchedule.Request) => Future[GetSchedule.Response],
) {
  abstract class BaseCommand[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
    override type Svc = Stub
    override def createService(channel: ManagedChannel): Svc = createServiceStub(channel)
  }

  // case classes not final as the scala compiler can't check outer Svc type reference
  case class SetScheduleCommand(
      cron: String,
      maxDuration: PositiveDurationSeconds,
      retention: PositiveDurationSeconds,
  ) extends BaseCommand[SetSchedule.Request, SetSchedule.Response, Unit] {
    override protected def createRequest(): Right[String, SetSchedule.Request] =
      Right(
        SetSchedule.Request(
          Some(
            PruningScheduleP(
              cron,
              Some(maxDuration.toProtoPrimitive),
              Some(retention.toProtoPrimitive),
            )
          )
        )
      )

    override protected def submitRequest(
        service: Svc,
        request: SetSchedule.Request,
    ): Future[SetSchedule.Response] = submitSetSchedule(service, request)

    override protected def handleResponse(response: SetSchedule.Response): Either[String, Unit] =
      response match {
        case SetSchedule.Response() => Either.unit
      }
  }

  case class ClearScheduleCommand()
      extends BaseCommand[ClearSchedule.Request, ClearSchedule.Response, Unit] {
    override protected def createRequest(): Right[String, ClearSchedule.Request] =
      Right(ClearSchedule.Request())

    override protected def submitRequest(
        service: Svc,
        request: ClearSchedule.Request,
    ): Future[ClearSchedule.Response] =
      submitClearSchedule(service, request)

    override protected def handleResponse(response: ClearSchedule.Response): Either[String, Unit] =
      response match {
        case ClearSchedule.Response() => Either.unit
      }
  }

  case class SetCronCommand(cron: String)
      extends BaseCommand[SetCron.Request, SetCron.Response, Unit] {
    override protected def createRequest(): Right[String, SetCron.Request] =
      Right(SetCron.Request(cron))

    override protected def submitRequest(
        service: Svc,
        request: SetCron.Request,
    ): Future[SetCron.Response] =
      submitSetCron(service, request)

    override protected def handleResponse(response: SetCron.Response): Either[String, Unit] =
      response match {
        case SetCron.Response() => Either.unit
      }
  }

  case class SetMaxDurationCommand(maxDuration: PositiveDurationSeconds)
      extends BaseCommand[SetMaxDuration.Request, SetMaxDuration.Response, Unit] {
    override protected def createRequest(): Right[String, SetMaxDuration.Request] =
      Right(
        SetMaxDuration.Request(Some(maxDuration.toProtoPrimitive))
      )

    override protected def submitRequest(
        service: Svc,
        request: SetMaxDuration.Request,
    ): Future[SetMaxDuration.Response] =
      submitSetMaxDuration(service, request)

    override protected def handleResponse(response: SetMaxDuration.Response): Either[String, Unit] =
      response match {
        case SetMaxDuration.Response() => Either.unit
      }
  }

  case class SetRetentionCommand(retention: PositiveDurationSeconds)
      extends BaseCommand[SetRetention.Request, SetRetention.Response, Unit] {
    override protected def createRequest(): Right[String, SetRetention.Request] =
      Right(SetRetention.Request(Some(retention.toProtoPrimitive)))

    override protected def submitRequest(
        service: Svc,
        request: SetRetention.Request,
    ): Future[SetRetention.Response] =
      submitSetRetention(service, request)

    override protected def handleResponse(response: SetRetention.Response): Either[String, Unit] =
      response match {
        case SetRetention.Response() => Either.unit
      }
  }

  case class GetScheduleCommand()
      extends BaseCommand[
        GetSchedule.Request,
        GetSchedule.Response,
        Option[PruningSchedule],
      ] {
    override protected def createRequest(): Right[String, GetSchedule.Request] =
      Right(GetSchedule.Request())

    override protected def submitRequest(
        service: Svc,
        request: GetSchedule.Request,
    ): Future[GetSchedule.Response] =
      submitGetSchedule(service, request)

    override protected def handleResponse(
        response: GetSchedule.Response
    ): Either[
      String,
      Option[PruningSchedule],
    ] = response.schedule.fold(
      Right(None): Either[String, Option[PruningSchedule]]
    )(PruningSchedule.fromProtoV30(_).bimap(_.message, Some(_)))
  }
}
