// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.implicits.{toBifunctorOps, toTraverseOps}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.WorkflowId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.admin.PingService
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.tracing.Spanning
import com.digitalasset.canton.util.OptionUtil
import io.opentelemetry.api.trace.Tracer

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class GrpcPingService(service: PingService, val loggerFactory: NamedLoggerFactory)(implicit
    ec: ExecutionContext,
    tracer: Tracer,
) extends PingServiceGrpc.PingService
    with NamedLogging
    with Spanning {

  override def ping(request: PingRequest): Future[PingResponse] =
    withSpanFromGrpcContext("GrpcPingService.ping") { implicit traceContext => span =>
      val PingRequest(
        targetPartiesP,
        validatorsP,
        timeoutP,
        levelsP,
        domainIdP,
        workflowIdP,
        idP,
      ) =
        request

      val result = for {
        targetParties <- targetPartiesP.traverse(PartyId.fromProtoPrimitive(_, "target_parties"))
        validators <- validatorsP.traverse(PartyId.fromProtoPrimitive(_, "validators"))
        timeout <- ProtoConverter.parseRequired(
          NonNegativeFiniteDuration.fromProtoPrimitive("timeout"),
          "timeout",
          timeoutP,
        )
        levels <- NonNegativeInt
          .create(levelsP)
          .leftMap(x => ProtoDeserializationError.ValueDeserializationError("levels", x.message))
        domainId <- OptionUtil
          .emptyStringAsNone(domainIdP)
          .traverse(DomainId.fromProtoPrimitive(_, "domain_id"))
      } yield {
        val id = if (request.id.isEmpty) UUID.randomUUID().toString else request.id
        val workflowId =
          if (request.workflowId.isEmpty) None
          else {
            span.setAttribute("workflow_id", request.workflowId)
            Some(WorkflowId(request.workflowId))
          }
        span.setAttribute("ping_id", id)
        service
          .ping(
            targetParties.toSet,
            validators.toSet,
            timeout,
            levels,
            domainId,
            workflowId,
            id,
          )
      }
      CantonGrpcUtil.mapErrNew(
        EitherT(
          result.fold(
            r => Future.successful(Left(ProtoDeserializationFailure.Wrap(r))),
            _.map(Right(_)),
          )
        ).map({
          case PingService.Success(millis, responder) =>
            PingResponse(PingResponse.Response.Success(PingSuccess(millis.toMillis, responder)))
          case PingService.Failure(reason) =>
            PingResponse(PingResponse.Response.Failure(PingFailure(reason)))
        })
      )
    }
}
