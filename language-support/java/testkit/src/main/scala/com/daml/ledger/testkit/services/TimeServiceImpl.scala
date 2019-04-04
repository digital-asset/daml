// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.testkit.services

import com.digitalasset.ledger.api.v1.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  SetTimeRequest,
  TimeServiceGrpc
}
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future};

class TimeServiceImpl(getTimeResponses: Seq[GetTimeResponse]) extends TimeService {

  private var lastGetTimeRequest: Option[GetTimeRequest] = None
  private var lastSetTimeRequest: Option[SetTimeRequest] = None

  override def getTime(
      request: GetTimeRequest,
      responseObserver: StreamObserver[GetTimeResponse]): Unit = {
    this.lastGetTimeRequest = Some(request)
    getTimeResponses.foreach(responseObserver.onNext)
    responseObserver.onCompleted()
  }

  override def setTime(request: SetTimeRequest): Future[Empty] = {
    this.lastSetTimeRequest = Some(request)
    Future.successful(Empty.defaultInstance)
  }

  def getLastGetTimeRequest: Option[GetTimeRequest] = this.lastGetTimeRequest

  def getLastSetTimeRequest: Option[SetTimeRequest] = this.lastSetTimeRequest
}

object TimeServiceImpl {
  def apply(getTimeResponses: GetTimeResponse*)(
      implicit ec: ExecutionContext): ServerServiceDefinition = {
    TimeServiceGrpc.bindService(new TimeServiceImpl(getTimeResponses), ec)
  }

  def createWithRef(getTimeResponses: GetTimeResponse*)(
      implicit ec: ExecutionContext): (ServerServiceDefinition, TimeServiceImpl) = {
    val time = new TimeServiceImpl(getTimeResponses)
    (TimeServiceGrpc.bindService(time, ec), time)
  }
}
