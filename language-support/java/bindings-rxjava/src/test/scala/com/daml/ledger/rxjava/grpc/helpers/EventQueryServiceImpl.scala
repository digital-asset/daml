// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.auth.services.EventQueryServiceAuthorization
import com.daml.ledger.api.v1.event_query_service.EventQueryServiceGrpc.EventQueryService
import com.daml.ledger.api.v1.event_query_service._
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class EventQueryServiceImpl(
    getEventsByContractIdResponse: Future[GetEventsByContractIdResponse],
    getEventsByContractKeyResponse: Future[GetEventsByContractKeyResponse],
) extends EventQueryService
    with FakeAutoCloseable {

  private var lastGetEventsByContractIdRequest: Option[GetEventsByContractIdRequest] = None
  private var lastGetEventsByContractKeyRequest: Option[GetEventsByContractKeyRequest] = None

  override def getEventsByContractId(request: GetEventsByContractIdRequest): Future[GetEventsByContractIdResponse] = {
    this.lastGetEventsByContractIdRequest = Some(request)
    getEventsByContractIdResponse
  }


  override def getEventsByContractKey(request: GetEventsByContractKeyRequest): Future[GetEventsByContractKeyResponse] = {
    this.lastGetEventsByContractKeyRequest = Some(request)
    getEventsByContractKeyResponse
  }

  def getLastGetEventsByContractIdRequest: Option[GetEventsByContractIdRequest] = this.lastGetEventsByContractIdRequest
  def getLastGetEventsByContractKeyRequest: Option[GetEventsByContractKeyRequest] = this.lastGetEventsByContractKeyRequest
}

object EventQueryServiceImpl {
  def createWithRef(
      getEventsByContractIdResponse: Future[GetEventsByContractIdResponse],
      getEventsByContractKeyResponse: Future[GetEventsByContractKeyResponse],
      authorizer: Authorizer,
  )(implicit ec: ExecutionContext): (ServerServiceDefinition, EventQueryServiceImpl) = {
    val impl =
      new EventQueryServiceImpl(getEventsByContractIdResponse,getEventsByContractKeyResponse)
    val authImpl = new EventQueryServiceAuthorization(impl, authorizer)
    (EventQueryServiceGrpc.bindService(authImpl, ec), impl)
  }
}
