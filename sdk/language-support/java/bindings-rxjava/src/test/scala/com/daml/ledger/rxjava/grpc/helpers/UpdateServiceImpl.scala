// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import java.util.concurrent.atomic.AtomicReference

import com.daml.ledger.api.v2.trace_context.TraceContext
import com.daml.ledger.rxjava.grpc.helpers.UpdateServiceImpl.LedgerItem
import com.digitalasset.canton.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.UpdateServiceAuthorization
import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.update_service.UpdateServiceGrpc.UpdateService
import com.daml.ledger.api.v2.update_service._
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver
import io.grpc.{Metadata, ServerServiceDefinition, Status}
import io.reactivex.disposables.Disposable
import io.reactivex.{Observable, Observer}

import scala.concurrent.{ExecutionContext, Future}

final class UpdateServiceImpl(ledgerContent: Observable[LedgerItem])
    extends UpdateService
    with FakeAutoCloseable {

  val lastUpdatesRequest = new AtomicReference[GetUpdatesRequest]()
  val lastUpdatesTreesRequest = new AtomicReference[GetUpdatesRequest]()
  val lastTransactionTreeByOffsetRequest = new AtomicReference[GetTransactionByOffsetRequest]()
  val lastTransactionTreeByIdRequest = new AtomicReference[GetTransactionByIdRequest]()
  val lastTransactionByOffsetRequest = new AtomicReference[GetTransactionByOffsetRequest]()
  val lastTransactionByIdRequest = new AtomicReference[GetTransactionByIdRequest]()

  override def getUpdates(
      request: GetUpdatesRequest,
      responseObserver: StreamObserver[GetUpdatesResponse],
  ): Unit = {
    lastUpdatesRequest.set(request)

    request.endInclusive match {
      case Some(endInclusive) if request.beginExclusive > endInclusive =>
        val metadata = new Metadata()
        metadata.put(
          Metadata.Key.of("cause", Metadata.ASCII_STRING_MARSHALLER),
          s"BEGIN should be strictly smaller than END. Found BEGIN '${request.beginExclusive}' and END '${request.endInclusive}'",
        )
        responseObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException(metadata))
      case _ =>
        ledgerContent.subscribe(new Observer[LedgerItem] {
          override def onSubscribe(d: Disposable): Unit = ()
          override def onNext(t: LedgerItem): Unit =
            responseObserver.onNext(
              GetUpdatesResponse(GetUpdatesResponse.Update.Transaction(t.toTransaction))
            )
          override def onError(t: Throwable): Unit = responseObserver.onError(t)
          override def onComplete(): Unit = responseObserver.onCompleted()
        })
    }
  }

  override def getUpdateTrees(
      request: GetUpdatesRequest,
      responseObserver: StreamObserver[GetUpdateTreesResponse],
  ): Unit = {
    lastUpdatesTreesRequest.set(request)
    responseObserver.onCompleted()
  }

  override def getTransactionTreeByOffset(
      request: GetTransactionByOffsetRequest
  ): Future[GetTransactionTreeResponse] = {
    lastTransactionTreeByOffsetRequest.set(request)
    Future.successful(
      new GetTransactionTreeResponse(None)
    ) // just a mock, not intended for consumption
  }

  override def getTransactionTreeById(
      request: GetTransactionByIdRequest
  ): Future[GetTransactionTreeResponse] = {
    lastTransactionTreeByIdRequest.set(request)
    Future.successful(
      new GetTransactionTreeResponse(None)
    ) // just a mock, not intended for consumption
  }

  override def getTransactionByOffset(
      request: GetTransactionByOffsetRequest
  ): Future[GetTransactionResponse] = {
    lastTransactionByOffsetRequest.set(request)
    Future.successful(
      new GetTransactionResponse(None)
    ) // just a mock, not intended for consumption
  }

  override def getTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetTransactionResponse] = {
    lastTransactionByIdRequest.set(request)
    Future.successful(
      new GetTransactionResponse(None)
    ) // just a mock, not intended for consumption
  }
}

object UpdateServiceImpl {

  final case class LedgerItem(
      updateId: String,
      commandId: String,
      workflowId: String,
      effectiveAt: Timestamp,
      events: Seq[Event],
      offset: Long,
      domainId: String,
      traceContext: TraceContext,
  ) {

    def toTransaction =
      Transaction(
        updateId,
        commandId,
        workflowId,
        Some(effectiveAt),
        events,
        offset,
        domainId,
        Some(traceContext),
      )
  }

  def createWithRef(ledgerContent: Observable[LedgerItem], authorizer: Authorizer)(implicit
      ec: ExecutionContext
  ): (ServerServiceDefinition, UpdateServiceImpl) = {
    val impl = new UpdateServiceImpl(ledgerContent)
    val authImpl = new UpdateServiceAuthorization(impl, authorizer)
    (UpdateServiceGrpc.bindService(authImpl, ec), impl)
  }
}
