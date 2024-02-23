// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import java.util.concurrent.atomic.AtomicReference

import com.daml.ledger.api.v1.trace_context.TraceContext
import com.daml.ledger.rxjava.grpc.helpers.UpdateServiceImpl.{LedgerItem, participantOffsetOrdering}
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.UpdateServiceAuthorization
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.event.Event.Event.{Archived, Created, Empty}
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset.ParticipantBoundary.{
  PARTICIPANT_BEGIN,
  PARTICIPANT_END,
  Unrecognized,
}
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
  val lastTransactionTreeByEventIdRequest = new AtomicReference[GetTransactionByEventIdRequest]()
  val lastTransactionTreeByIdRequest = new AtomicReference[GetTransactionByIdRequest]()
  val lastTransactionByEventIdRequest = new AtomicReference[GetTransactionByEventIdRequest]()
  val lastTransactionByIdRequest = new AtomicReference[GetTransactionByIdRequest]()

  override def getUpdates(
      request: GetUpdatesRequest,
      responseObserver: StreamObserver[GetUpdatesResponse],
  ): Unit = {
    lastUpdatesRequest.set(request)

    if (
      request.endInclusive
        .exists(end => participantOffsetOrdering.gt(request.getBeginExclusive, end))
    ) {
      val metadata = new Metadata()
      metadata.put(
        Metadata.Key.of("cause", Metadata.ASCII_STRING_MARSHALLER),
        s"BEGIN should be strictly smaller than END. Found BEGIN '${request.getBeginExclusive}' and END '${request.getEndInclusive}'",
      )
      responseObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException(metadata))
    } else {
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

  override def getTransactionTreeByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetTransactionTreeResponse] = {
    lastTransactionTreeByEventIdRequest.set(request)
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

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetTransactionResponse] = {
    lastTransactionByEventIdRequest.set(request)
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
      offset: String,
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

  def eventId(event: Event): String = event.event match {
    case Archived(archivedEvent) => archivedEvent.eventId
    case Created(createdEvent) => createdEvent.eventId
    case Empty => ""
  }

  def createWithRef(ledgerContent: Observable[LedgerItem], authorizer: Authorizer)(implicit
      ec: ExecutionContext
  ): (ServerServiceDefinition, UpdateServiceImpl) = {
    val impl = new UpdateServiceImpl(ledgerContent)
    val authImpl = new UpdateServiceAuthorization(impl, authorizer)
    (UpdateServiceGrpc.bindService(authImpl, ec), impl)
  }

  val participantOffsetOrdering: Ordering[ParticipantOffset] =
    (x: ParticipantOffset, y: ParticipantOffset) => {
      if (x.equals(y)) 0
      else {
        x.getAbsolute match {
          case "" =>
            x.getBoundary match {
              case PARTICIPANT_BEGIN => -1
              case PARTICIPANT_END => 1
              case Unrecognized(value) =>
                throw new RuntimeException(
                  s"Found boundary that is neither BEGIN or END (value: $value)"
                )
            }
          case xAbs =>
            y.getAbsolute match {
              case "" =>
                y.getBoundary match {
                  case PARTICIPANT_BEGIN => 1
                  case PARTICIPANT_END => -1
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
