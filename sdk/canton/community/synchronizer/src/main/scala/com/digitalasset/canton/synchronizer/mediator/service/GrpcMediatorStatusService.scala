// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.service

import com.digitalasset.canton.admin.mediator.v30 as mediatorV30
import com.digitalasset.canton.admin.mediator.v30.MediatorStatusRequest
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.mediator.admin.data.MediatorNodeStatus
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.Future

class GrpcMediatorStatusService(
    status: => NodeStatus[MediatorNodeStatus],
    val loggerFactory: NamedLoggerFactory,
) extends mediatorV30.MediatorStatusServiceGrpc.MediatorStatusService
    with NamedLogging {

  override def mediatorStatus(
      request: MediatorStatusRequest
  ): Future[mediatorV30.MediatorStatusResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val responseP: mediatorV30.MediatorStatusResponse.Kind = status match {
      case NodeStatus.Failure(_msg) =>
        logger.warn(s"Unexpectedly found failure status: ${_msg}")
        mediatorV30.MediatorStatusResponse.Kind.Empty

      case notInitialized: NodeStatus.NotInitialized =>
        mediatorV30.MediatorStatusResponse.Kind.NotInitialized(notInitialized.toProtoV30)

      case NodeStatus.Success(status: MediatorNodeStatus) =>
        mediatorV30.MediatorStatusResponse.Kind.Status(status.toMediatorStatusProto)
    }

    Future.successful(mediatorV30.MediatorStatusResponse(responseP))
  }

}
