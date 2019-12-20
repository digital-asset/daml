// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.validation

import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

class ActiveContractsServiceValidation(
    protected val service: ActiveContractsService with AutoCloseable,
    val ledgerId: LedgerId)
    extends ActiveContractsService
    with ProxyCloseable
    with GrpcApiService
    with FieldValidations {

  protected val logger: Logger = LoggerFactory.getLogger(ActiveContractsService.getClass)

  override def getActiveContracts(
      request: GetActiveContractsRequest,
      responseObserver: StreamObserver[GetActiveContractsResponse]): Unit = {
    matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      .fold(responseObserver.onError, _ => service.getActiveContracts(request, responseObserver))
  }
  override def bindService(): ServerServiceDefinition =
    ActiveContractsServiceGrpc.bindService(this, DirectExecutionContext)
}
