// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.health

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.grpc.{GrpcException, GrpcStatus}
import com.digitalasset.platform.sandbox.health.HealthService._
import io.grpc.health.v1.{HealthCheckRequest, HealthCheckResponse, HealthGrpc}
import io.grpc.stub.StreamObserver
import io.grpc.{Context, Status}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class HealthService(watchThrottleFrequency: FiniteDuration = 1.second)(
    implicit materializer: Materializer,
    executionContext: ExecutionContext
) extends HealthGrpc.HealthImplBase {
  private val servingResponse = HealthCheckResponse
    .newBuilder()
    .setStatus(HealthCheckResponse.ServingStatus.SERVING)
    .build()

  override def check(
      request: HealthCheckRequest,
      responseObserver: StreamObserver[HealthCheckResponse],
  ): Unit = {
    responseObserver.onNext(servingResponse)
    responseObserver.onCompleted()
  }

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
      .via(dropRepeated)
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
          suppress(responseObserver.onError(error))
      }
  }
}

object HealthService {
  private def suppress(f: => Unit): Unit =
    try {
      f
    } catch {
      case NonFatal(_) => // ignore
    }
}
