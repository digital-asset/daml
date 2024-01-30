// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.TimeServiceV2Authorization
import com.daml.ledger.api.v2.testing.time_service.TimeServiceGrpc.TimeService
import com.daml.ledger.api.v2.testing.time_service.{GetTimeRequest, GetTimeResponse, SetTimeRequest, TimeServiceGrpc}
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}


final class TimeServiceImpl(getTimeResponse: Future[GetTimeResponse])
    extends TimeService
    with FakeAutoCloseable {

  private var lastGetTimeRequest: Option[GetTimeRequest] = None
  private var lastSetTimeRequest: Option[SetTimeRequest] = None

  override def getTime(
      request: GetTimeRequest,
  ): Future[GetTimeResponse] = {
    this.lastGetTimeRequest = Some(request)
    getTimeResponse
  }

  override def setTime(request: SetTimeRequest): Future[Empty] = {
    this.lastSetTimeRequest = Some(request)
    Future.successful(Empty.defaultInstance)
  }

  def getLastGetTimeRequest: Option[GetTimeRequest] = this.lastGetTimeRequest

  def getLastSetTimeRequest: Option[SetTimeRequest] = this.lastSetTimeRequest
}

object TimeServiceImpl {
  def createWithRef(getTimeResponse: Future[GetTimeResponse], authorizer: Authorizer)(implicit
      ec: ExecutionContext
  ): (ServerServiceDefinition, TimeServiceImpl) = {
    val impl = new TimeServiceImpl(getTimeResponse)
    val authImpl = new TimeServiceV2Authorization(impl, authorizer)
    (TimeServiceGrpc.bindService(authImpl, ec), impl)
  }
}
