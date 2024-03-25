// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset.ParticipantBoundary.PARTICIPANT_BOUNDARY_BEGIN
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset.Value.{Absolute, Boundary}
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.StateServiceAuthorization
import com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateService
import com.daml.ledger.api.v2.state_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse,
  GetConnectedDomainsRequest,
  GetConnectedDomainsResponse,
  GetLatestPrunedOffsetsRequest,
  GetLatestPrunedOffsetsResponse,
  GetLedgerEndRequest,
  GetLedgerEndResponse,
  StateServiceGrpc,
}
import com.daml.ledger.rxjava.grpc.helpers.UpdateServiceImpl.LedgerItem
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import io.reactivex.disposables.Disposable
import io.reactivex.{Observable, Observer}

import scala.concurrent.{ExecutionContext, Future, Promise}

final class StateServiceImpl(
    getActiveContractsResponses: Observable[GetActiveContractsResponse],
    ledgerContent: Observable[LedgerItem],
) extends StateService
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

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] = {
    val promise = Promise[GetLedgerEndResponse]()
    val result =
      ledgerContent
        .map[GetLedgerEndResponse](t =>
          GetLedgerEndResponse(Option(ParticipantOffset(Absolute(t.offset))))
        )
        .last(GetLedgerEndResponse(Option(ParticipantOffset(Boundary(PARTICIPANT_BOUNDARY_BEGIN)))))
    result.subscribe(promise.success _, promise.failure _)
    promise.future
  }

  override def getConnectedDomains(
      request: GetConnectedDomainsRequest
  ): Future[GetConnectedDomainsResponse] = ???

  override def getLatestPrunedOffsets(
      request: GetLatestPrunedOffsetsRequest
  ): Future[GetLatestPrunedOffsetsResponse] = ???
}

object StateServiceImpl {

  /** Return the ServerServiceDefinition and the underlying ActiveContractsServiceImpl for inspection */
  def createWithRef(
      getActiveContractsResponses: Observable[GetActiveContractsResponse],
      ledgerContent: Observable[LedgerItem],
      authorizer: Authorizer,
  )(implicit ec: ExecutionContext): (ServerServiceDefinition, StateServiceImpl) = {
    val impl = new StateServiceImpl(getActiveContractsResponses, ledgerContent)
    val authImpl = new StateServiceAuthorization(impl, authorizer)
    (StateServiceGrpc.bindService(authImpl, ec), impl)
  }

}
