// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.event_query_service.EventQueryServiceGrpc.EventQueryService
import com.daml.ledger.api.v2.event_query_service.{
  EventQueryServiceGrpc,
  GetEventsByContractIdRequest,
  GetEventsByContractIdResponse,
}
import com.digitalasset.canton.auth.Authorizer
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class EventQueryServiceAuthorization(
    protected val service: EventQueryService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends EventQueryService
    with ProxyCloseable
    with GrpcApiService {

  override def getEventsByContractId(
      request: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getEventsByContractId,
    )(request)

  override def bindService(): ServerServiceDefinition =
    EventQueryServiceGrpc.bindService(this, executionContext)
}
