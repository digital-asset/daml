// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.participant.admin.{ResourceLimits, ResourceManagementService}
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.empty.Empty

import scala.concurrent.{ExecutionContext, Future}

class GrpcResourceManagementService(
    service: ResourceManagementService,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends v30.ResourceManagementServiceGrpc.ResourceManagementService
    with NamedLogging {

  override def updateResourceLimits(limitsP: v30.ResourceLimits): Future[Empty] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      val limits = ResourceLimits.fromProtoV30(limitsP)
      service.writeResourceLimits(limits).map(_ => Empty()).asGrpcResponse
    }

  override def getResourceLimits(request: Empty): Future[v30.ResourceLimits] =
    Future.successful(service.resourceLimits.toProtoV30)
}
