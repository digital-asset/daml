// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService
import com.daml.ledger.api.v1.ledger_configuration_service.*
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContext

final class LedgerConfigurationServiceAuthorization(
    protected val service: LedgerConfigurationService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends LedgerConfigurationService
    with ProxyCloseable
    with GrpcApiService {

  override def getLedgerConfiguration(
      request: GetLedgerConfigurationRequest,
      responseObserver: StreamObserver[GetLedgerConfigurationResponse],
  ): Unit =
    authorizer.requirePublicClaimsOnStream(service.getLedgerConfiguration)(
      request,
      responseObserver,
    )

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}
