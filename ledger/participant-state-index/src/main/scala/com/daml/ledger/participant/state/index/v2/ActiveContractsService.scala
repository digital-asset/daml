// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.digitalasset.ledger.api.domain.TransactionFilter

import scala.concurrent.Future

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService]]
  **/
trait ActiveContractsService {

  def getActiveContractSetSnapshot(
      filter: TransactionFilter
  ): Future[ActiveContractSetSnapshot]
}
