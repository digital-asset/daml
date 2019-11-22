// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.health

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.platform.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import io.grpc.health.v1.health.{
  HealthAkkaGrpc,
  HealthCheckRequest,
  HealthCheckResponse,
  HealthGrpc
}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class HealthService(
    implicit protected val esf: ExecutionSequencerFactory,
    protected val mat: Materializer,
    executionContext: ExecutionContext,
) extends HealthAkkaGrpc
    with GrpcApiService {
  private val servingResponse = HealthCheckResponse(HealthCheckResponse.ServingStatus.SERVING)

  override def bindService(): ServerServiceDefinition =
    HealthGrpc.bindService(this, DirectExecutionContext)

  override def check(request: HealthCheckRequest): Future[HealthCheckResponse] =
    Future.successful(servingResponse)

  override def watchSource(request: HealthCheckRequest): Source[HealthCheckResponse, NotUsed] =
    Source
      .repeat(servingResponse)
      .throttle(1, per = 1.second)
      .via(DropRepeated())
}
