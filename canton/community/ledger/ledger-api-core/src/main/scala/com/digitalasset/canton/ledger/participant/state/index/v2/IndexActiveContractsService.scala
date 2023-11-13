// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.digitalasset.canton.ledger.api.domain.TransactionFilter
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService]]
  */
trait IndexActiveContractsService {

  def getActiveContracts(
      filter: TransactionFilter,
      verbose: Boolean,
      activeAtO: Option[Offset],
      multiDomainEnabled: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetActiveContractsResponse, NotUsed]
}
