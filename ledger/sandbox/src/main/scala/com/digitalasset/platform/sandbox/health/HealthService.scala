// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.health

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import io.grpc.health.v1.{HealthCheckRequest, HealthCheckResponse, HealthGrpc}
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

class HealthService(implicit materializer: Materializer, executionContext: ExecutionContext)
    extends HealthGrpc.HealthImplBase {
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
    Source
      .repeat(servingResponse)
      .throttle(1, per = 1.second)
      .runWith(Sink.foreach(responseObserver.onNext))
      .onComplete {
        case Success(Done) => responseObserver.onCompleted()
        case Failure(error) => responseObserver.onError(error)
      }
  }
}
