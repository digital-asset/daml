// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import com.digitalasset.canton.health.NodeStatus
import com.digitalasset.canton.health.admin.{v0, v1}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.ParticipantStatus
import com.digitalasset.canton.participant.admin.v0.{
  ParticipantStatusRequest,
  ParticipantStatusResponse,
  ParticipantStatusServiceGrpc,
}

import scala.concurrent.Future

class GrpcParticipantStatusService(
    status: => NodeStatus[ParticipantStatus],
    val loggerFactory: NamedLoggerFactory,
) extends ParticipantStatusServiceGrpc.ParticipantStatusService
    with NamedLogging {

  override def participantStatus(
      request: ParticipantStatusRequest
  ): Future[ParticipantStatusResponse] = {

    val responseP: ParticipantStatusResponse.Kind = status match {
      case NodeStatus.Failure(msg) =>
        ParticipantStatusResponse.Kind.Failure(v1.Failure(msg))

      case NodeStatus.NotInitialized(active) =>
        ParticipantStatusResponse.Kind.Unavailable(v0.NodeStatus.NotInitialized(active))

      case NodeStatus.Success(status: ParticipantStatus) =>
        ParticipantStatusResponse.Kind.Status(status.toParticipantStatusProto)
    }

    Future.successful(ParticipantStatusResponse(responseP))
  }
}
