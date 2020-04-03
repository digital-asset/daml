// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.ledger.api.domain.TransactionFilter
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService]]
  **/
trait IndexActiveContractsService {

  def getActiveContracts(
      filter: TransactionFilter,
      verbose: Boolean,
  ): Source[GetActiveContractsResponse, NotUsed]
}
