// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import com.digitalasset.canton.admin.participant.v0.*
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.WorkflowId
import com.digitalasset.canton.participant.admin.PingService
import com.digitalasset.canton.tracing.Spanning
import io.opentelemetry.api.trace.Tracer

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class GrpcPingService(service: PingService)(implicit ec: ExecutionContext, tracer: Tracer)
    extends PingServiceGrpc.PingService
    with Spanning {

  override def ping(request: PingRequest): Future[PingResponse] =
    withSpanFromGrpcContext("GrpcPingService.ping") { implicit traceContext => span =>
      val workflowId =
        if (request.workflowId.isEmpty) None
        else {
          span.setAttribute("workflow_id", request.workflowId)
          Some(WorkflowId(request.workflowId))
        }
      val id = if (request.id.isEmpty) UUID.randomUUID().toString else request.id
      span.setAttribute("ping_id", id)

      val result =
        service
          .ping(
            request.targetParties.toSet,
            request.validators.toSet,
            request.timeoutMilliseconds,
            request.gracePeriodMilliseconds,
            request.levels,
            workflowId,
            id,
          )

      result.map({
        case PingService.Success(millis, responder) =>
          PingResponse(PingResponse.Response.Success(PingSuccess(millis.toMillis, responder)))
        case PingService.Failure =>
          PingResponse(PingResponse.Response.Failure(PingFailure()))
      })
    }
}
