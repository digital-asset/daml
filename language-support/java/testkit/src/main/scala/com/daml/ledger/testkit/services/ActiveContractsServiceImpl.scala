// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.testkit.services

import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import io.reactivex.disposables.Disposable
import io.reactivex.{Observable, Observer}

import collection.JavaConverters._

import scala.concurrent.ExecutionContext

class ActiveContractsServiceImpl(
    getActiveContractsResponses: Observable[GetActiveContractsResponse])
    extends ActiveContractsService {

  private var lastRequest = Option.empty[GetActiveContractsRequest]

  override def getActiveContracts(
      request: GetActiveContractsRequest,
      responseObserver: StreamObserver[GetActiveContractsResponse]): Unit = {
    lastRequest = Option(request)
    getActiveContractsResponses.subscribe(new Observer[GetActiveContractsResponse] {
      override def onSubscribe(d: Disposable): Unit = ()
      override def onNext(t: GetActiveContractsResponse): Unit = responseObserver.onNext(t)
      override def onError(e: Throwable): Unit = responseObserver.onError(e)
      override def onComplete(): Unit = responseObserver.onCompleted()
    })
  }

  def getLastRequest: Option[GetActiveContractsRequest] = this.lastRequest
}

object ActiveContractsServiceImpl {
  def apply(getActiveContractsResponses: GetActiveContractsResponse*)(
      implicit ec: ExecutionContext): ServerServiceDefinition =
    ActiveContractsServiceGrpc.bindService(
      new ActiveContractsServiceImpl(Observable.fromIterable(getActiveContractsResponses.asJava)),
      ec)

  /** Return the ServerServiceDefinition and the underlying ActiveContractsServiceImpl for inspection */
  def createWithRef(getActiveContractsResponses: Observable[GetActiveContractsResponse])(
      implicit ec: ExecutionContext): (ServerServiceDefinition, ActiveContractsServiceImpl) = {
    val acsImpl = new ActiveContractsServiceImpl(getActiveContractsResponses)
    (ActiveContractsServiceGrpc.bindService(acsImpl, ec), acsImpl)
  }
}
