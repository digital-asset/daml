// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v1.transaction_service
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.daml.ledger.api.v1.transaction_service.*
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

final class TransactionServiceAuthorization(
    protected val service: TransactionService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends TransactionService
    with ProxyCloseable
    with GrpcApiService {

  override def getTransactions(
      request: transaction_service.GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionsResponse],
  ): Unit =
    authorizer.requireReadClaimsForTransactionFilterOnStream(
      request.filter.map(_.filtersByParty),
      service.getTransactions,
    )(request, responseObserver)

  override def getTransactionTrees(
      request: transaction_service.GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionTreesResponse],
  ): Unit =
    authorizer.requireReadClaimsForTransactionFilterOnStream(
      request.filter.map(_.filtersByParty),
      service.getTransactionTrees,
    )(request, responseObserver)

  override def getTransactionByEventId(
      request: transaction_service.GetTransactionByEventIdRequest
  ): Future[GetTransactionResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getTransactionByEventId,
    )(request)

  override def getTransactionById(
      request: transaction_service.GetTransactionByIdRequest
  ): Future[GetTransactionResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getTransactionById,
    )(request)

  override def getFlatTransactionByEventId(
      request: transaction_service.GetTransactionByEventIdRequest
  ): Future[GetFlatTransactionResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getFlatTransactionByEventId,
    )(request)

  override def getFlatTransactionById(
      request: transaction_service.GetTransactionByIdRequest
  ): Future[GetFlatTransactionResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getFlatTransactionById,
    )(request)

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] =
    authorizer.requirePublicClaims(service.getLedgerEnd)(request)

  override def getLatestPrunedOffsets(
      request: GetLatestPrunedOffsetsRequest
  ): Future[GetLatestPrunedOffsetsResponse] =
    authorizer.requirePublicClaims(service.getLatestPrunedOffsets)(request)

  override def bindService(): ServerServiceDefinition =
    TransactionServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}
