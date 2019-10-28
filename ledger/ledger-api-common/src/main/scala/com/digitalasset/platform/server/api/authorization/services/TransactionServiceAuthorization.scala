// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.authorization.services

import com.digitalasset.ledger.api.auth.AuthService
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.v1.transaction_service
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetLedgerEndRequest,
  GetLedgerEndResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
  TransactionServiceGrpc
}
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import com.digitalasset.platform.server.api.authorization.ApiServiceAuthorization
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class TransactionServiceAuthorization(
    protected val service: TransactionService with AutoCloseable,
    protected val authService: AuthService)
    extends TransactionService
    with ProxyCloseable
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(TransactionService.getClass)

  override def getTransactions(
      request: transaction_service.GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionsResponse]): Unit =
    // TODO(RA): To implement claims expiration, get the expiration date here and create a
    // wrapping StreamObserver that checks the expiration date on each callback.
    // OR: overwrite getTransactionsSource from [[GrpcTransactionService]] and kill the source
    // as soon as the claims expire.
    ApiServiceAuthorization
      .requireClaimsForTransactionFilter(request.filter)
      .fold(responseObserver.onError(_), _ => service.getTransactions(request, responseObserver))

  override def getTransactionTrees(
      request: transaction_service.GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionTreesResponse]): Unit =
    ApiServiceAuthorization
      .requireClaimsForTransactionFilter(request.filter)
      .fold(
        responseObserver.onError(_),
        _ => service.getTransactionTrees(request, responseObserver))

  override def getTransactionByEventId(
      request: transaction_service.GetTransactionByEventIdRequest): Future[GetTransactionResponse] =
    ApiServiceAuthorization
      .requireClaimsForAllParties(request.requestingParties.toSet)
      .fold(Future.failed(_), _ => service.getTransactionByEventId(request))

  override def getTransactionById(
      request: transaction_service.GetTransactionByIdRequest): Future[GetTransactionResponse] =
    ApiServiceAuthorization
      .requireClaimsForAllParties(request.requestingParties.toSet)
      .fold(Future.failed(_), _ => service.getTransactionById(request))

  override def getFlatTransactionByEventId(
      request: transaction_service.GetTransactionByEventIdRequest)
    : Future[GetFlatTransactionResponse] =
    ApiServiceAuthorization
      .requireClaimsForAllParties(request.requestingParties.toSet)
      .fold(Future.failed(_), _ => service.getFlatTransactionByEventId(request))

  override def getFlatTransactionById(
      request: transaction_service.GetTransactionByIdRequest): Future[GetFlatTransactionResponse] =
    ApiServiceAuthorization
      .requireClaimsForAllParties(request.requestingParties.toSet)
      .fold(Future.failed(_), _ => service.getFlatTransactionById(request))

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] =
    ApiServiceAuthorization
      .requirePublicClaims()
      .fold(Future.failed(_), _ => service.getLedgerEnd(request))

  override def bindService(): ServerServiceDefinition =
    TransactionServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
