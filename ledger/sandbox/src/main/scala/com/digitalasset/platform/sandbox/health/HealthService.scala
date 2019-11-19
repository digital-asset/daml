// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.health

import io.grpc.health.v1.{HealthCheckRequest, HealthCheckResponse, HealthGrpc}
import io.grpc.stub.StreamObserver

class HealthService extends HealthGrpc.HealthImplBase {
  override def check(
      request: HealthCheckRequest,
      responseObserver: StreamObserver[HealthCheckResponse],
  ): Unit = {
    responseObserver.onNext(
      HealthCheckResponse
        .newBuilder()
        .setStatus(HealthCheckResponse.ServingStatus.SERVING)
        .build())
    responseObserver.onCompleted()
  }
}
