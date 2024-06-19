// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.LedgerTransactionId
import com.digitalasset.canton.admin.participant.v30.InspectionServiceGrpc.InspectionService
import com.digitalasset.canton.admin.participant.v30.{
  GetConfigForSlowCounterParticipants,
  GetIntervalsBehindForCounterParticipants,
  LookupContractDomain,
  LookupOffsetByIndex,
  LookupOffsetByTime,
  LookupReceivedAcsCommitments,
  LookupSentAcsCommitments,
  LookupTransactionDomain,
  SetConfigForSlowCounterParticipants,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.FutureInstances.*
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

class GrpcInspectionService(syncStateInspection: SyncStateInspection)(implicit
    executionContext: ExecutionContext
) extends InspectionService {

  override def lookupContractDomain(
      request: LookupContractDomain.Request
  ): Future[LookupContractDomain.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    for {
      contractIds <- request.contractId.parTraverse(cid =>
        Future.successful( // Future, because GRPC expects a failed future in case of an error
          LfContractId
            .fromString(cid)
            .valueOr(err =>
              throw Status.INVALID_ARGUMENT
                .withDescription(err)
                .asRuntimeException()
            )
        )
      )
      domainsByContractId <- syncStateInspection.lookupContractDomain(contractIds.toSet)
    } yield {
      LookupContractDomain.Response(
        domainsByContractId.map { case (contractId, alias) => contractId.coid -> alias.unwrap }
      )
    }
  }

  override def lookupTransactionDomain(
      request: LookupTransactionDomain.Request
  ): Future[LookupTransactionDomain.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    LedgerTransactionId.fromString(request.transactionId) match {
      case Left(err) =>
        Future.failed(
          new IllegalArgumentException(
            s"""String "${request.transactionId}" doesn't parse as a transaction ID: $err"""
          )
        )
      case Right(txId) =>
        syncStateInspection.lookupTransactionDomain(txId).map { domainId =>
          LookupTransactionDomain.Response(
            domainId.fold(throw new StatusRuntimeException(Status.NOT_FOUND))(_.toProtoPrimitive)
          )
        }
    }
  }

  override def lookupOffsetByTime(
      request: LookupOffsetByTime.Request
  ): Future[LookupOffsetByTime.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    request.timestamp.fold[Future[LookupOffsetByTime.Response]](
      Future.failed(new IllegalArgumentException(s"""Timestamp not specified"""))
    ) { ts =>
      CantonTimestamp.fromProtoTimestamp(ts) match {
        case Right(cantonTimestamp) =>
          syncStateInspection
            .getOffsetByTime(cantonTimestamp)
            .map(ledgerOffset => LookupOffsetByTime.Response(ledgerOffset.fold("")(_.getAbsolute)))
        case Left(err) =>
          Future.failed(new IllegalArgumentException(s"""Failed to parse timestamp: $err"""))
      }
    }
  }

  override def lookupOffsetByIndex(
      request: LookupOffsetByIndex.Request
  ): Future[LookupOffsetByIndex.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    if (request.index <= 0) {
      Future.failed(
        new IllegalArgumentException(s"""Index needs to be positive and not ${request.index}""")
      )
    } else {
      syncStateInspection
        .locateOffset(request.index)
        .map(
          _.fold(
            err =>
              throw new StatusRuntimeException(
                Status.OUT_OF_RANGE.withDescription(s"""Failed to locate offset: $err""")
              ),
            ledgerOffset => LookupOffsetByIndex.Response(ledgerOffset.getAbsolute),
          )
        )
    }
  }

  /** Configure metrics for slow counter-participants (i.e., that are behind in sending commitments) and
    * configure thresholds for when a counter-participant is deemed slow.
    * TODO(#10436) R7
    */
  override def setConfigForSlowCounterParticipants(
      request: SetConfigForSlowCounterParticipants.Request
  ): Future[SetConfigForSlowCounterParticipants.Response] = ???

  /** Get the current configuration for metrics for slow counter-participants.
    * TODO(#10436) R7
    */
  override def getConfigForSlowCounterParticipants(
      request: GetConfigForSlowCounterParticipants.Request
  ): Future[GetConfigForSlowCounterParticipants.Response] = ???

  /** Get the number of intervals that counter-participants are behind in sending commitments.
    * Can be used to decide whether to ignore slow counter-participants w.r.t. pruning.
    * TODO(#10436) R7
    */
  override def getIntervalsBehindForCounterParticipants(
      request: GetIntervalsBehindForCounterParticipants.Request
  ): Future[GetIntervalsBehindForCounterParticipants.Response] = ???

  /** TODO(#18452) R5
    * Look up the ACS commitments computed and sent by a participant
    */
  override def lookupSentAcsCommitments(
      request: LookupSentAcsCommitments.Request
  ): Future[LookupSentAcsCommitments.Response] = ???

  /** TODO(#18452) R5
    * List the counter-participants of a participant and their ACS commitments together with the match status
    * TODO(#18749) R1 Can also be used for R1, to fetch commitments that a counter participant received from myself
    */
  override def lookupReceivedAcsCommitments(
      request: LookupReceivedAcsCommitments.Request
  ): Future[LookupReceivedAcsCommitments.Response] = ???
}
