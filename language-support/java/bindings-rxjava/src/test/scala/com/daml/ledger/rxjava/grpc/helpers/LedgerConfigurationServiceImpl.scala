// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.auth.services.LedgerConfigurationServiceAuthorization
import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService
import com.daml.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse,
  LedgerConfigurationServiceGrpc,
}
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContext

class LedgerConfigurationServiceImpl(responses: Seq[GetLedgerConfigurationResponse])
    extends LedgerConfigurationService
    with FakeAutoCloseable {

  private var lastRequest: Option[GetLedgerConfigurationRequest] = None

  override def getLedgerConfiguration(
      request: GetLedgerConfigurationRequest,
      responseObserver: StreamObserver[GetLedgerConfigurationResponse],
  ): Unit = {
    this.lastRequest = Some(request)
    responses.foreach(responseObserver.onNext)
  }

  def getLastRequest: Option[GetLedgerConfigurationRequest] = this.lastRequest
}

object LedgerConfigurationServiceImpl {
  def createWithRef(responses: Seq[GetLedgerConfigurationResponse], authorizer: Authorizer)(implicit
      ec: ExecutionContext
  ): (ServerServiceDefinition, LedgerConfigurationServiceImpl) = {
    val impl = new LedgerConfigurationServiceImpl(responses)
    val authImpl = new LedgerConfigurationServiceAuthorization(impl, authorizer)
    (LedgerConfigurationServiceGrpc.bindService(authImpl, ec), impl)
  }
}
