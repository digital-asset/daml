// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.update_service.UpdateServiceGrpc.UpdateService
import com.daml.ledger.api.v2.update_service.*
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

final class UpdateServiceAuthorization(
    protected val service: UpdateService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends UpdateService
    with ProxyCloseable
    with GrpcApiService {

  override def bindService(): ServerServiceDefinition =
    UpdateServiceGrpc.bindService(this, executionContext)

  override def getUpdates(
      request: GetUpdatesRequest,
      responseObserver: StreamObserver[GetUpdatesResponse],
  ): Unit =
    authorizer.requireReadClaimsForTransactionFilterOnStream(
      request.filter.map(_.filtersByParty),
      request.filter.flatMap(_.filtersForAnyParty).nonEmpty,
      service.getUpdates,
    )(request, responseObserver)

  override def getUpdateTrees(
      request: GetUpdatesRequest,
      responseObserver: StreamObserver[GetUpdateTreesResponse],
  ): Unit =
    authorizer.requireReadClaimsForTransactionFilterOnStream(
      request.filter.map(_.filtersByParty),
      request.filter.flatMap(_.filtersForAnyParty).nonEmpty,
      service.getUpdateTrees,
    )(request, responseObserver)

  override def getTransactionTreeByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetTransactionTreeResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getTransactionTreeByEventId,
    )(request)

  override def getTransactionTreeById(
      request: GetTransactionByIdRequest
  ): Future[GetTransactionTreeResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getTransactionTreeById,
    )(request)

  override def getTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[GetTransactionResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getTransactionByEventId,
    )(request)

  override def getTransactionById(
      request: GetTransactionByIdRequest
  ): Future[GetTransactionResponse] =
    authorizer.requireReadClaimsForAllParties(
      request.requestingParties,
      service.getTransactionById,
    )(request)
}
