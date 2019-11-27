// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.grpc

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.health.HealthChecks
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.DropRepeated
import com.digitalasset.platform.server.api.services.grpc.GrpcHealthService._
import io.grpc.health.v1.health.{
  HealthAkkaGrpc,
  HealthCheckRequest,
  HealthCheckResponse,
  HealthGrpc
}
import io.grpc.{ServerServiceDefinition, Status, StatusException}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

class GrpcHealthService(
    healthChecks: HealthChecks,
    maximumWatchFrequency: FiniteDuration = 1.second,
)(
    implicit protected val esf: ExecutionSequencerFactory,
    protected val mat: Materializer,
    executionContext: ExecutionContext,
) extends HealthAkkaGrpc
    with GrpcApiService {
  override def bindService(): ServerServiceDefinition =
    HealthGrpc.bindService(this, DirectExecutionContext)

  override def check(request: HealthCheckRequest): Future[HealthCheckResponse] =
    Future(matchResponse(Option(request.service).filter(_.nonEmpty)))

  override def watchSource(request: HealthCheckRequest): Source[HealthCheckResponse, NotUsed] =
    Source
      .fromIterator(() =>
        Iterator.continually(matchResponse(Option(request.service).filter(_.nonEmpty))))
      .throttle(1, per = maximumWatchFrequency)
      .via(DropRepeated())

  private def matchResponse(componentName: Option[String]): HealthCheckResponse = {
    if (!componentName.forall(healthChecks.hasComponent))
      throw new StatusException(Status.NOT_FOUND)
    if (healthChecks.isHealthy(componentName))
      servingResponse
    else
      notServingResponse
  }
}

object GrpcHealthService {
  private[grpc] val servingResponse =
    HealthCheckResponse(HealthCheckResponse.ServingStatus.SERVING)

  private[grpc] val notServingResponse =
    HealthCheckResponse(HealthCheckResponse.ServingStatus.NOT_SERVING)
}
