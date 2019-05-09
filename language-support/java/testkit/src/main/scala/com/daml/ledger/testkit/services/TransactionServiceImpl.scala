// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.testkit.services

import java.util.concurrent.atomic.AtomicReference

import com.daml.ledger.testkit.services.TransactionServiceImpl.LedgerItem
import com.digitalasset.ledger.api.v1.event.Event
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created, Empty}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.{
  LEDGER_BEGIN,
  LEDGER_END,
  Unrecognized
}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value.{Absolute, Boundary}
import com.digitalasset.ledger.api.v1.trace_context.TraceContext
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.digitalasset.ledger.api.v1.transaction_service._
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver
import io.grpc.{Metadata, ServerServiceDefinition, Status, StatusRuntimeException}
import io.reactivex.disposables.Disposable
import io.reactivex.{Observable, Observer, Single}

import scala.concurrent.{ExecutionContext, Future, Promise}

import TransactionServiceImpl.{ledgerOffsetOrdering, rxSingleWrapper}

class TransactionServiceImpl(ledgerContent: Observable[LedgerItem]) extends TransactionService {

  val lastTransactionsRequest = new AtomicReference[GetTransactionsRequest]()
  val lastTransactionsTreesRequest = new AtomicReference[GetTransactionsRequest]()
  val lastTransactionByEventIdRequest = new AtomicReference[GetTransactionByEventIdRequest]()
  val lastTransactionByIdRequest = new AtomicReference[GetTransactionByIdRequest]()
  val lastFlatTransactionByEventIdRequest = new AtomicReference[GetTransactionByEventIdRequest]()
  val lastFlatTransactionByIdRequest = new AtomicReference[GetTransactionByIdRequest]()
  val lastLedgerEndRequest = new AtomicReference[GetLedgerEndRequest]()

  override def getTransactions(
      request: GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionsResponse]): Unit = {
    lastTransactionsRequest.set(request)

    if (request.end.exists(end => ledgerOffsetOrdering.gt(request.getBegin, end))) {
      val metadata = new Metadata()
      metadata.put(
        Metadata.Key.of("cause", Metadata.ASCII_STRING_MARSHALLER),
        s"BEGIN should be strictly smaller than END. Found BEGIN '${request.getBegin}' and END '${request.getEnd}'"
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
      responseObserver: StreamObserver[GetTransactionTreesResponse]): Unit = {
    lastTransactionsTreesRequest.set(request)
//  TODO DEL-6007
//    ledgerContent
//      .map(t => GetTransactionTreesResponse(List(t.toTransactionTree)))
//      .foreach(responseObserver.onNext)

    responseObserver.onCompleted()
  }

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[GetTransactionResponse] =
    Future.failed[GetTransactionResponse] {
      lastTransactionByEventIdRequest.set(request)
      //  TODO DEL-6007
      new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Pending"))
    }

  override def getTransactionById(
      request: GetTransactionByIdRequest): Future[GetTransactionResponse] =
    Future.failed[GetTransactionResponse] {
      lastTransactionByIdRequest.set(request)
      //  TODO DEL-6007
      new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Pending"))
    }

  override def getFlatTransactionByEventId(
      request: GetTransactionByEventIdRequest): Future[GetFlatTransactionResponse] =
    Future.failed[GetFlatTransactionResponse] {
      lastFlatTransactionByEventIdRequest.set(request)
      //  TODO DEL-6007
      new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Pending"))
    }

  override def getFlatTransactionById(
      request: GetTransactionByIdRequest): Future[GetFlatTransactionResponse] =
    Future.failed[GetFlatTransactionResponse] {
      lastFlatTransactionByIdRequest.set(request)
      //  TODO DEL-6007
      new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Pending"))
    }

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] = {
    lastLedgerEndRequest.set(request)

    ledgerContent
      .map[GetLedgerEndResponse](t =>
        GetLedgerEndResponse(Option(LedgerOffset(Absolute(t.offset)))))
      .last(GetLedgerEndResponse(Option(LedgerOffset(Boundary(LEDGER_BEGIN)))))
      .toScalaFuture
  }

}

object TransactionServiceImpl {

  case class LedgerItem(
      transactionId: String,
      commandId: String,
      workflowId: String,
      effectiveAt: Timestamp,
      events: Seq[Event],
      offset: String,
      traceContext: Option[TraceContext]) {

    def toTransaction =
      Transaction(
        transactionId,
        commandId,
        workflowId,
        Some(effectiveAt),
        events,
        offset,
        traceContext)
// TODO DEL-6007
//    def toTransactionTree =
//      TransactionTree(transactionId,
//                      commandId,
//                      workflowId,
//                      Some(effectiveAt),
//                      events,
//                      offset,
//                      traceContext)

  }

  def eventId(event: Event): String = event.event match {
    case Archived(archivedEvent) => archivedEvent.eventId
    case Created(createdEvent) => createdEvent.eventId
    case Empty => ""
  }

  def createWithRef(ledgerContent: Observable[LedgerItem])(
      implicit ec: ExecutionContext): (ServerServiceDefinition, TransactionServiceImpl) = {
    val serviceImpl = new TransactionServiceImpl(ledgerContent)
    (TransactionServiceGrpc.bindService(serviceImpl, ec), serviceImpl)
  }

  val ledgerOffsetOrdering: Ordering[LedgerOffset] = new Ordering[LedgerOffset] {
    override def compare(x: LedgerOffset, y: LedgerOffset): Int = {
      if (x.equals(y)) 0
      else {
        x.getAbsolute match {
          case "" =>
            x.getBoundary match {
              case LEDGER_BEGIN => -1
              case LEDGER_END => 1
              case Unrecognized(value) =>
                throw new RuntimeException(
                  s"Found boundary that is neither BEGIN or END (value: $value)")
            }
          case xAbs =>
            y.getAbsolute match {
              case "" =>
                y.getBoundary match {
                  case LEDGER_BEGIN => 1
                  case LEDGER_END => -1
                  case Unrecognized(value) =>
                    throw new RuntimeException(
                      s"Found boundary that is neither BEGIN or END (value: $value)")
                }
              case yAbs => xAbs.compareTo(yAbs)
            }
        }
      }
    }
  }

  def singleToFuture[A](single: Single[A]): Future[A] = {
    val promise = Promise[A]()
    single.subscribe(promise.success, promise.failure)
    promise.future
  }

  implicit class rxSingleWrapper[A](val single: Single[A]) extends AnyVal {
    def toScalaFuture: Future[A] = singleToFuture(single)
  }
}
