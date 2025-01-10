// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.admin.{ResourceLimits, ResourceManagementService}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.{ExecutionContext, Future}

class GrpcResourceManagementService(
    service: ResourceManagementService,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends v30.ResourceManagementServiceGrpc.ResourceManagementService
    with NamedLogging {

  override def setResourceLimits(
      requestP: v30.SetResourceLimitsRequest
  ): Future[v30.SetResourceLimitsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {
      newLimits <- ProtoConverter
        .parseRequired(ResourceLimits.fromProtoV30, "new_limits", requestP.newLimits)
        .toEitherT[FutureUnlessShutdown]
      _ <- EitherT.right[ProtoDeserializationError](service.writeResourceLimits(newLimits))
    } yield v30.SetResourceLimitsResponse()

    CantonGrpcUtil.mapErrNewEUS(result.leftMap(ProtoDeserializationFailure.Wrap.apply))
  }

  override def getResourceLimits(
      request: v30.GetResourceLimitsRequest
  ): Future[v30.GetResourceLimitsResponse] =
    Future.successful(
      v30.GetResourceLimitsResponse(currentLimits = Some(service.resourceLimits.toProtoV30))
    )
}
