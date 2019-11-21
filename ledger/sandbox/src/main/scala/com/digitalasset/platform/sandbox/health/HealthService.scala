// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.health

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.grpc.{GrpcException, GrpcStatus}
import com.digitalasset.platform.api.grpc.GrpcApiService
import io.grpc.health.v1.health.{HealthCheckRequest, HealthCheckResponse, HealthGrpc}
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ServerServiceDefinition, Status}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class HealthService(watchThrottleFrequency: FiniteDuration = 1.second)(
    implicit materializer: Materializer,
    executionContext: ExecutionContext,
) extends HealthGrpc.Health
    with GrpcApiService {
  private val servingResponse = HealthCheckResponse(HealthCheckResponse.ServingStatus.SERVING)

  override def bindService(): ServerServiceDefinition =
    HealthGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = ()

  override def check(request: HealthCheckRequest): Future[HealthCheckResponse] =
    Future.successful(servingResponse)

  override def watch(
      request: HealthCheckRequest,
      responseObserver: StreamObserver[HealthCheckResponse],
  ): Unit = {
    val context = Context.current()
    watch(request, responseObserver, () => context.isCancelled)
  }

  private[health] def watch(
      request: HealthCheckRequest,
      responseObserver: StreamObserver[HealthCheckResponse],
      isCancelled: () => Boolean,
  ): Unit = {
    Source
      .repeat(servingResponse)
      .takeWhile(_ => !isCancelled())
      .throttle(1, per = watchThrottleFrequency)
      .via(DropRepeated())
      .runWith(Sink.foreach(responseObserver.onNext))
      .onComplete {
        case Success(Done) =>
          try {
            responseObserver.onCompleted()
          } catch {
            // The stream was cancelled; don't worry about it.
            case GrpcException(GrpcStatus(Status.Code.CANCELLED, _), _) =>
          }
        case Failure(error) =>
          try {
            responseObserver.onError(error)
          } catch {
            // ignore; what else can we do?
            case NonFatal(_) =>
          }
      }
  }
}
