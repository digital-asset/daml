// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.daml.ledger.api.v1.event_query_service.GetEventsByContractIdRequest
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.EventQueryServiceV2Authorization
import com.daml.ledger.api.v2.event_query_service.EventQueryServiceGrpc.EventQueryService
import com.daml.ledger.api.v2.event_query_service.{
  EventQueryServiceGrpc,
  GetEventsByContractIdResponse,
}
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class EventQueryServiceImpl(
    getEventsByContractIdResponse: Future[GetEventsByContractIdResponse]
) extends EventQueryService
    with FakeAutoCloseable {

  private var lastGetEventsByContractIdRequest: Option[GetEventsByContractIdRequest] = None

  override def getEventsByContractId(
      request: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse] = {
    this.lastGetEventsByContractIdRequest = Some(request)
    getEventsByContractIdResponse
  }

  def getLastGetEventsByContractIdRequest: Option[GetEventsByContractIdRequest] =
    this.lastGetEventsByContractIdRequest
}

object EventQueryServiceImpl {
  def createWithRef(
      getEventsByContractIdResponse: Future[GetEventsByContractIdResponse],
      authorizer: Authorizer,
  )(implicit ec: ExecutionContext): (ServerServiceDefinition, EventQueryServiceImpl) = {
    val impl =
      new EventQueryServiceImpl(getEventsByContractIdResponse)
    val authImpl = new EventQueryServiceV2Authorization(impl, authorizer)
    (EventQueryServiceGrpc.bindService(authImpl, ec), impl)
  }
}
