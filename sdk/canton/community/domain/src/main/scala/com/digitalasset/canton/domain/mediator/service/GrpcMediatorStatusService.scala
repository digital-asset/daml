// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.service

import com.digitalasset.canton.admin.domain.v30 as domainV30
import com.digitalasset.canton.admin.domain.v30.MediatorStatusRequest
import com.digitalasset.canton.domain.mediator.admin.data.MediatorNodeStatus
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.Future

class GrpcMediatorStatusService(
    status: => NodeStatus[MediatorNodeStatus],
    val loggerFactory: NamedLoggerFactory,
) extends domainV30.MediatorStatusServiceGrpc.MediatorStatusService
    with NamedLogging {

  override def mediatorStatus(
      request: MediatorStatusRequest
  ): Future[domainV30.MediatorStatusResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val responseP: domainV30.MediatorStatusResponse.Kind = status match {
      case NodeStatus.Failure(_msg) =>
        logger.warn(s"Unexpectedly found failure status: ${_msg}")
        domainV30.MediatorStatusResponse.Kind.Empty

      case notInitialized: NodeStatus.NotInitialized =>
        domainV30.MediatorStatusResponse.Kind.NotInitialized(notInitialized.toProtoV30)

      case NodeStatus.Success(status: MediatorNodeStatus) =>
        domainV30.MediatorStatusResponse.Kind.Status(status.toMediatorStatusProto)
    }

    Future.successful(domainV30.MediatorStatusResponse(responseP))
  }

}
