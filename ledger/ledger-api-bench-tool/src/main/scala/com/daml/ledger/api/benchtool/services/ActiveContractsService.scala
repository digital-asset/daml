// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.Config
import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.v1.active_contracts_service._
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
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
      config: Config.StreamConfig.ActiveContractsStreamConfig,
      observer: ObserverWithResult[GetActiveContractsResponse, Result],
  ): Future[Result] = {
    service.getActiveContracts(getActiveContractsRequest(ledgerId, config), observer)
    logger.info("Started fetching active contracts")
    observer.result
  }

  private def getActiveContractsRequest(
      ledgerId: String,
      config: Config.StreamConfig.ActiveContractsStreamConfig,
  ) = {
    val templatesFilter = config.templateIds match {
      case Some(ids) =>
        Filters.defaultInstance.withInclusive(
          InclusiveFilters.defaultInstance.addAllTemplateIds(ids)
        )
      case None =>
        Filters.defaultInstance
    }

    GetActiveContractsRequest.defaultInstance
      .withLedgerId(ledgerId)
      .withFilter(
        TransactionFilter.defaultInstance
          .withFiltersByParty(
            Map(
              config.party -> templatesFilter
            )
          )
      )
  }

}
