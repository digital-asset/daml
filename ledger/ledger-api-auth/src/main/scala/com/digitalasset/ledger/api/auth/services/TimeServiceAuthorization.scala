// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth.services

import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.auth.Authorizer
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.digitalasset.ledger.api.v1.testing.time_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

final class TimeServiceAuthorization(
    protected val service: TimeService with AutoCloseable,
    private val authorizer: Authorizer)
    extends TimeService
    with ProxyCloseable
    with GrpcApiService {

  override def getTime(
      request: GetTimeRequest,
      responseObserver: StreamObserver[GetTimeResponse]): Unit =
    authorizer.requirePublicClaimsOnStream(service.getTime)(request, responseObserver)

  override def setTime(request: SetTimeRequest): Future[Empty] =
    authorizer.requireAdminClaims(service.setTime)(request)

  override def bindService(): ServerServiceDefinition =
    TimeServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
