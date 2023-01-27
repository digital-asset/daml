// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import java.util.concurrent.atomic.AtomicReference

import com.daml.ledger.rxjava.grpc.helpers.TransactionsServiceImpl.{
  LedgerItem,
  ledgerOffsetOrdering,
}
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.auth.services.TransactionServiceAuthorization
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.event.Event.Event.{Archived, Created, Empty}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.{
  LEDGER_BEGIN,
  LEDGER_END,
  Unrecognized,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.Value.{Absolute, Boundary}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.daml.ledger.api.v1.transaction_service._
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver
import io.grpc.{Metadata, ServerServiceDefinition, Status}
import io.reactivex.disposables.Disposable
import io.reactivex.{Observable, Observer}

import scala.concurrent.{ExecutionContext, Future, Promise}

final class TransactionsServiceImpl(ledgerContent: Observable[LedgerItem])
    extends TransactionService
    with FakeAutoCloseable {

  val lastTransactionsRequest = new AtomicReference[GetTransactionsRequest]()
  val lastTransactionsTreesRequest = new AtomicReference[GetTransactionsRequest]()
  val lastTransactionByEventIdRequest = new AtomicReference[GetTransactionByEventIdRequest]()
  val lastTransactionByIdRequest = new AtomicReference[GetTransactionByIdRequest]()
  val lastFlatTransactionByEventIdRequest = new AtomicReference[GetTransactionByEventIdRequest]()
  val lastFlatTransactionByIdRequest = new AtomicReference[GetTransactionByIdRequest]()
  val lastLedgerEndRequest = new AtomicReference[GetLedgerEndRequest]()
  val lastLatestPrunedOffsetsRequest = new AtomicReference[GetLatestPrunedOffsetsRequest]

  override def getTransactions(
      request: GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionsResponse],
  ): Unit = {
    lastTransactionsRequest.set(request)

    if (request.end.exists(end => ledgerOffsetOrdering.gt(request.getBegin, end))) {
      val metadata = new Metadata()
      metadata.put(
        Metadata.Key.of("cause", Metadata.ASCII_STRING_MARSHALLER),
        s"BEGIN should be strictly smaller than END. Found BEGIN '${request.getBegin}' and END '${request.getEnd}'",
      )
      responseObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException(metadata))
    } else {
      ledgerContent.subscribe(new Observer[LedgerItem] {
        override def onSubscribe(d: Disposable): Unit = ()
        override def onNext(t: LedgerItem): Unit =
          responseObserver.onNext(GetTransactionsResponse(List(t.toTransaction)))
        override def onError(t: Throwable): Unit = responseObserver.onError(t)
        override def onComplete(): Unit = responseObserver.onCompleted()
      })
    }
  }

  override def getTransactionTrees(
      request: GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionTreesResponse],
  ): Unit = {
    lastTransactionsTreesRequest.set(request)
    responseObserver.onCompleted()
  }

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetTransactionResponse] = {
    lastTransactionByEventIdRequest.set(request)
    Future.successful(new GetTransactionResponse(None)) // just a mock, not intended for consumption
  }

  override def getTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetTransactionResponse] = {
    lastTransactionByIdRequest.set(request)
    Future.successful(new GetTransactionResponse(None)) // just a mock, not intended for consumption
  }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetFlatTransactionResponse] = {
    lastFlatTransactionByEventIdRequest.set(request)
    Future.successful(
      new GetFlatTransactionResponse(None)
    ) // just a mock, not intended for consumption
  }

  override def getFlatTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetFlatTransactionResponse] = {
    lastFlatTransactionByIdRequest.set(request)
    Future.successful(
      new GetFlatTransactionResponse(None)
    ) // just a mock, not intended for consumption
  }

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] = {
    lastLedgerEndRequest.set(request)
    val promise = Promise[GetLedgerEndResponse]()
    val result =
      ledgerContent
        .map[GetLedgerEndResponse](t =>
          GetLedgerEndResponse(Option(LedgerOffset(Absolute(t.offset))))
        )
        .last(GetLedgerEndResponse(Option(LedgerOffset(Boundary(LEDGER_BEGIN)))))
    result.subscribe(promise.success _, promise.failure _)
    promise.future
  }

  override def getLatestPrunedOffsets(
      request: GetLatestPrunedOffsetsRequest
  ): Future[GetLatestPrunedOffsetsResponse] = {
    lastLatestPrunedOffsetsRequest.set(request)
    Future.successful(
      new GetLatestPrunedOffsetsResponse(None)
    ) // just a mock, not intended for consumption
  }

}

object TransactionsServiceImpl {

  final case class LedgerItem(
      transactionId: String,
      commandId: String,
      workflowId: String,
      effectiveAt: Timestamp,
      events: Seq[Event],
      offset: String,
  ) {

    def toTransaction =
      Transaction(
        transactionId,
        commandId,
        workflowId,
        Some(effectiveAt),
        events,
        offset,
      )
  }

  def eventId(event: Event): String = event.event match {
    case Archived(archivedEvent) => archivedEvent.eventId
    case Created(createdEvent) => createdEvent.eventId
    case Empty => ""
  }

  def createWithRef(ledgerContent: Observable[LedgerItem], authorizer: Authorizer)(implicit
      ec: ExecutionContext
  ): (ServerServiceDefinition, TransactionsServiceImpl) = {
    val impl = new TransactionsServiceImpl(ledgerContent)
    val authImpl = new TransactionServiceAuthorization(impl, authorizer)
    (TransactionServiceGrpc.bindService(authImpl, ec), impl)
  }

  val ledgerOffsetOrdering: Ordering[LedgerOffset] = (x: LedgerOffset, y: LedgerOffset) => {
    if (x.equals(y)) 0
    else {
      x.getAbsolute match {
        case "" =>
          x.getBoundary match {
            case LEDGER_BEGIN => -1
            case LEDGER_END => 1
            case Unrecognized(value) =>
              throw new RuntimeException(
                s"Found boundary that is neither BEGIN or END (value: $value)"
              )
          }
        case xAbs =>
          y.getAbsolute match {
            case "" =>
              y.getBoundary match {
                case LEDGER_BEGIN => 1
                case LEDGER_END => -1
                case Unrecognized(value) =>
                  throw new RuntimeException(
                    s"Found boundary that is neither BEGIN or END (value: $value)"
                  )
              }
            case yAbs => xAbs.compareTo(yAbs)
          }
      }
    }
  }
}
