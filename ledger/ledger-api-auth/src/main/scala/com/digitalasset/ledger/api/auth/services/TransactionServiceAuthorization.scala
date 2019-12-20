// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth.services

import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.auth.Authorizer
import com.digitalasset.ledger.api.v1.transaction_service
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.digitalasset.ledger.api.v1.transaction_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

final class TransactionServiceAuthorization(
    protected val service: TransactionService with AutoCloseable,
    private val authorizer: Authorizer)
    extends TransactionService
    with ProxyCloseable
    with GrpcApiService {

  override def getTransactions(
      request: transaction_service.GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionsResponse]): Unit =
    authorizer.requireReadClaimsForTransactionFilterOnStream(
      request.filter,
      service.getTransactions)(request, responseObserver)

  override def getTransactionTrees(
      request: transaction_service.GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionTreesResponse]): Unit =
    authorizer.requireReadClaimsForTransactionFilterOnStream(
      request.filter,
      service.getTransactionTrees)(request, responseObserver)

  override def getTransactionByEventId(
      request: transaction_service.GetTransactionByEventIdRequest): Future[GetTransactionResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getTransactionByEventId)(request)

  override def getTransactionById(
      request: transaction_service.GetTransactionByIdRequest): Future[GetTransactionResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getTransactionById)(request)

  override def getFlatTransactionByEventId(
      request: transaction_service.GetTransactionByEventIdRequest)
    : Future[GetFlatTransactionResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getFlatTransactionByEventId)(request)

  override def getFlatTransactionById(
      request: transaction_service.GetTransactionByIdRequest): Future[GetFlatTransactionResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getFlatTransactionById)(request)

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] =
    authorizer.requirePublicClaims(service.getLedgerEnd)(request)

  override def bindService(): ServerServiceDefinition =
    TransactionServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
