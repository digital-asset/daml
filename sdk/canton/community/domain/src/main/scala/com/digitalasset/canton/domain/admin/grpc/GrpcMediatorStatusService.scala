// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.grpc

import com.digitalasset.canton.domain.admin.data.MediatorNodeStatus
import com.digitalasset.canton.domain.admin.v0.MediatorStatusRequest
import com.digitalasset.canton.domain.admin.v0 as domainV0
import com.digitalasset.canton.health.NodeStatus
import com.digitalasset.canton.health.admin.{v0, v1}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}

import scala.concurrent.Future

class GrpcMediatorStatusService(
    status: => NodeStatus[MediatorNodeStatus],
    val loggerFactory: NamedLoggerFactory,
) extends domainV0.MediatorStatusServiceGrpc.MediatorStatusService
    with NamedLogging {

  override def mediatorStatus(
      request: MediatorStatusRequest
  ): Future[domainV0.MediatorStatusResponse] = {

    val responseP: domainV0.MediatorStatusResponse.Kind = status match {
      case NodeStatus.Failure(msg) =>
        domainV0.MediatorStatusResponse.Kind.Failure(v1.Failure(msg))

      case NodeStatus.NotInitialized(active) =>
        domainV0.MediatorStatusResponse.Kind.Unavailable(v0.NodeStatus.NotInitialized(active))

      case NodeStatus.Success(status: MediatorNodeStatus) =>
        domainV0.MediatorStatusResponse.Kind.Status(status.toMediatorStatusProto)
    }

    Future.successful(domainV0.MediatorStatusResponse(responseP))
  }

}
