// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.authorization.services

import com.digitalasset.ledger.api.auth.AuthService
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.digitalasset.ledger.api.v1.testing.time_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import com.digitalasset.platform.server.api.authorization.ApiServiceAuthorization
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class TimeServiceAuthorization(
    protected val service: TimeService with AutoCloseable,
    protected val authService: AuthService)
    extends TimeService
    with ProxyCloseable
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(TimeService.getClass)

  override def getTime(
      request: GetTimeRequest,
      responseObserver: StreamObserver[GetTimeResponse]): Unit =
    ApiServiceAuthorization
      .requirePublicClaims()
      .fold(responseObserver.onError, _ => service.getTime(request, responseObserver))

  override def setTime(request: SetTimeRequest): Future[Empty] =
    ApiServiceAuthorization
      .requireAdminClaims()
      .fold(Future.failed(_), _ => service.setTime(request))

  override def bindService(): ServerServiceDefinition =
    TimeServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
