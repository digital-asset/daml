// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  * admin api. Having to type-parameterize as grpc does not support inheritance and passing in the
  * grpc stub methods in one by one
  */
@GrpcServiceInvocationMethod
class PruningSchedulerCommands[Stub <: AbstractStub[Stub]](
    createServiceStub: ManagedChannel => Stub,
    submitSetSchedule: (Stub, SetScheduleRequest) => Future[SetScheduleResponse],
    submitClearSchedule: (Stub, ClearScheduleRequest) => Future[ClearScheduleResponse],
    submitSetCron: (Stub, SetCronRequest) => Future[SetCronResponse],
    submitSetMaxDuration: (Stub, v30.SetMaxDurationRequest) => Future[SetMaxDurationResponse],
    submitSetRetention: (Stub, SetRetentionRequest) => Future[SetRetentionResponse],
    submitGetSchedule: (Stub, GetScheduleRequest) => Future[GetScheduleResponse],
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
  ) extends BaseCommand[SetScheduleRequest, SetScheduleResponse, Unit] {
    override protected def createRequest(): Right[String, SetScheduleRequest] =
      Right(
        SetScheduleRequest(
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
        request: SetScheduleRequest,
    ): Future[SetScheduleResponse] = submitSetSchedule(service, request)

    override protected def handleResponse(response: SetScheduleResponse): Either[String, Unit] =
      response match {
        case SetScheduleResponse() => Either.unit
      }
  }

  case class ClearScheduleCommand()
      extends BaseCommand[ClearScheduleRequest, ClearScheduleResponse, Unit] {
    override protected def createRequest(): Right[String, ClearScheduleRequest] =
      Right(ClearScheduleRequest())

    override protected def submitRequest(
        service: Svc,
        request: ClearScheduleRequest,
    ): Future[ClearScheduleResponse] =
      submitClearSchedule(service, request)

    override protected def handleResponse(response: ClearScheduleResponse): Either[String, Unit] =
      response match {
        case ClearScheduleResponse() => Either.unit
      }
  }

  case class SetCronCommand(cron: String)
      extends BaseCommand[SetCronRequest, SetCronResponse, Unit] {
    override protected def createRequest(): Right[String, SetCronRequest] =
      Right(SetCronRequest(cron))

    override protected def submitRequest(
        service: Svc,
        request: SetCronRequest,
    ): Future[SetCronResponse] =
      submitSetCron(service, request)

    override protected def handleResponse(response: SetCronResponse): Either[String, Unit] =
      response match {
        case SetCronResponse() => Either.unit
      }
  }

  case class SetMaxDurationCommand(maxDuration: PositiveDurationSeconds)
      extends BaseCommand[SetMaxDurationRequest, SetMaxDurationResponse, Unit] {
    override protected def createRequest(): Right[String, SetMaxDurationRequest] =
      Right(
        SetMaxDurationRequest(Some(maxDuration.toProtoPrimitive))
      )

    override protected def submitRequest(
        service: Svc,
        request: SetMaxDurationRequest,
    ): Future[SetMaxDurationResponse] =
      submitSetMaxDuration(service, request)

    override protected def handleResponse(response: SetMaxDurationResponse): Either[String, Unit] =
      response match {
        case SetMaxDurationResponse() => Either.unit
      }
  }

  case class SetRetentionCommand(retention: PositiveDurationSeconds)
      extends BaseCommand[SetRetentionRequest, SetRetentionResponse, Unit] {
    override protected def createRequest(): Right[String, SetRetentionRequest] =
      Right(SetRetentionRequest(Some(retention.toProtoPrimitive)))

    override protected def submitRequest(
        service: Svc,
        request: SetRetentionRequest,
    ): Future[SetRetentionResponse] =
      submitSetRetention(service, request)

    override protected def handleResponse(response: SetRetentionResponse): Either[String, Unit] =
      response match {
        case SetRetentionResponse() => Either.unit
      }
  }

  case class GetScheduleCommand()
      extends BaseCommand[
        GetScheduleRequest,
        GetScheduleResponse,
        Option[PruningSchedule],
      ] {
    override protected def createRequest(): Right[String, GetScheduleRequest] =
      Right(GetScheduleRequest())

    override protected def submitRequest(
        service: Svc,
        request: GetScheduleRequest,
    ): Future[GetScheduleResponse] =
      submitGetSchedule(service, request)

    override protected def handleResponse(
        response: GetScheduleResponse
    ): Either[
      String,
      Option[PruningSchedule],
    ] = response.schedule.fold(
      Right(None): Either[String, Option[PruningSchedule]]
    )(PruningSchedule.fromProtoV30(_).bimap(_.message, Some(_)))
  }
}
