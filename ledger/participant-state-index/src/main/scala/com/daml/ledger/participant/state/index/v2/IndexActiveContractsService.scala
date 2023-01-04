// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.domain.TransactionFilter
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService]]
  */
trait IndexActiveContractsService {

  def getActiveContracts(
      filter: TransactionFilter,
      verbose: Boolean,
      activeAtO: Option[Offset],
  )(implicit loggingContext: LoggingContext): Source[GetActiveContractsResponse, NotUsed]
}
