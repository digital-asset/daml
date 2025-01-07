// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import com.digitalasset.canton.admin.participant.v30.{
  ParticipantStatusRequest,
  ParticipantStatusResponse,
  ParticipantStatusServiceGrpc,
}
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.health.admin.ParticipantStatus
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.Future

class GrpcParticipantStatusService(
    status: => NodeStatus[ParticipantStatus],
    val loggerFactory: NamedLoggerFactory,
) extends ParticipantStatusServiceGrpc.ParticipantStatusService
    with NamedLogging {

  override def participantStatus(
      request: ParticipantStatusRequest
  ): Future[ParticipantStatusResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val responseP: ParticipantStatusResponse.Kind = status match {
      case NodeStatus.Failure(_msg) =>
        logger.warn(s"Unexpectedly found failure status: ${_msg}")
        ParticipantStatusResponse.Kind.Empty

      case notInitialized: NodeStatus.NotInitialized =>
        ParticipantStatusResponse.Kind.NotInitialized(notInitialized.toProtoV30)

      case NodeStatus.Success(status: ParticipantStatus) =>
        ParticipantStatusResponse.Kind.Status(status.toParticipantStatusProto)
    }

    Future.successful(ParticipantStatusResponse(responseP))
  }
}
