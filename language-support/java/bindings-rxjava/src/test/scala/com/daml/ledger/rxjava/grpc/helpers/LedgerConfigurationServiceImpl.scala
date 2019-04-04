// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.digitalasset.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse,
  LedgerConfigurationServiceGrpc
}
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContext

class LedgerConfigurationServiceImpl(responses: Seq[GetLedgerConfigurationResponse])
    extends LedgerConfigurationService {

  private var lastRequest: Option[GetLedgerConfigurationRequest] = None

  override def getLedgerConfiguration(
      request: GetLedgerConfigurationRequest,
      responseObserver: StreamObserver[GetLedgerConfigurationResponse]): Unit = {
    this.lastRequest = Some(request)
    responses.foreach(responseObserver.onNext)
  }

  def getLastRequest: Option[GetLedgerConfigurationRequest] = this.lastRequest
}

object LedgerConfigurationServiceImpl {
  def createWithRef(responses: Seq[GetLedgerConfigurationResponse])(
      implicit ec: ExecutionContext): (ServerServiceDefinition, LedgerConfigurationServiceImpl) = {
    val serviceImpl = new LedgerConfigurationServiceImpl(responses)
    (LedgerConfigurationServiceGrpc.bindService(serviceImpl, ec), serviceImpl)
  }
}
