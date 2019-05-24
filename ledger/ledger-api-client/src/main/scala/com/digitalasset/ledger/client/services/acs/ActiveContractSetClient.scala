// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.acs

import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.active_contracts_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.digitalasset.ledger.api.v1.transaction_filter.TransactionFilter

import scala.concurrent.Future

import scalaz.syntax.tag._

class ActiveContractSetClient(ledgerId: LedgerId, activeContractsService: ActiveContractsService)(
    implicit esf: ExecutionSequencerFactory) {
  def getActiveContracts(
      filter: TransactionFilter,
      verbose: Boolean = false): Source[GetActiveContractsResponse, Future[String]] = {
    ActiveContractSetSource(
      activeContractsService.getActiveContracts,
      GetActiveContractsRequest(ledgerId.unwrap, Some(filter), verbose))
  }
}
