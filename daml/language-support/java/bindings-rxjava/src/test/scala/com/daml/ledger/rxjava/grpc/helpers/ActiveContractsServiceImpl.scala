// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.auth.services.ActiveContractsServiceAuthorization
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.daml.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse,
}
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import io.reactivex.disposables.Disposable
import io.reactivex.{Observable, Observer}

import scala.concurrent.ExecutionContext

final class ActiveContractsServiceImpl(
    getActiveContractsResponses: Observable[GetActiveContractsResponse]
) extends ActiveContractsService
    with FakeAutoCloseable {

  private var lastRequest = Option.empty[GetActiveContractsRequest]

  override def getActiveContracts(
      request: GetActiveContractsRequest,
      responseObserver: StreamObserver[GetActiveContractsResponse],
  ): Unit = {
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

  /** Return the ServerServiceDefinition and the underlying ActiveContractsServiceImpl for inspection */
  def createWithRef(
      getActiveContractsResponses: Observable[GetActiveContractsResponse],
      authorizer: Authorizer,
  )(implicit ec: ExecutionContext): (ServerServiceDefinition, ActiveContractsServiceImpl) = {
    val impl = new ActiveContractsServiceImpl(getActiveContractsResponses)
    val authImpl = new ActiveContractsServiceAuthorization(impl, authorizer)
    (ActiveContractsServiceGrpc.bindService(authImpl, ec), impl)
  }

}
