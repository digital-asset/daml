// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.daml.ledger.api.v1.testing.time_service._
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
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
