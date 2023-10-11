// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.testing.time_service.TimeServiceGrpc.TimeService
import com.daml.ledger.api.v2.testing.time_service.*
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class TimeServiceV2Authorization(
    protected val service: TimeService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends TimeService
    with ProxyCloseable
    with GrpcApiService {

  override def getTime(request: GetTimeRequest): Future[GetTimeResponse] =
    authorizer.requirePublicClaims(service.getTime)(request)

  override def setTime(request: SetTimeRequest): Future[Empty] =
    authorizer.requireAdminClaims(service.setTime)(request)

  override def bindService(): ServerServiceDefinition =
    TimeServiceGrpc.bindService(this, executionContext)
}
