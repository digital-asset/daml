// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.WorkflowConfig
import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.v1.active_contracts_service._
import io.grpc.Channel
import org.slf4j.LoggerFactory

import scala.concurrent.Future

final class ActiveContractsService(
    channel: Channel,
    ledgerId: String,
) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val service: ActiveContractsServiceGrpc.ActiveContractsServiceStub =
    ActiveContractsServiceGrpc.stub(channel)

  def getActiveContracts[Result](
      config: WorkflowConfig.StreamConfig.ActiveContractsStreamConfig,
      observer: ObserverWithResult[GetActiveContractsResponse, Result],
  ): Future[Result] = {
    service.getActiveContracts(getActiveContractsRequest(ledgerId, config), observer)
    logger.info("Started fetching active contracts")
    observer.result
  }

  private def getActiveContractsRequest(
      ledgerId: String,
      config: WorkflowConfig.StreamConfig.ActiveContractsStreamConfig,
  ): GetActiveContractsRequest =
    GetActiveContractsRequest.defaultInstance
      .withLedgerId(ledgerId)
      .withFilter(StreamFilters.transactionFilters(config.filters))

}
