// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.digitalasset.canton.data.AbsoluteOffset
import com.digitalasset.canton.ledger.api.domain.TransactionFilter
import com.digitalasset.canton.logging.LoggingContextWithTrace
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateService]]
  */
trait IndexActiveContractsService {

  def getActiveContracts(
      filter: TransactionFilter,
      verbose: Boolean,
      activeAt: Option[AbsoluteOffset],
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetActiveContractsResponse, NotUsed]
}
