// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.acs

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsServiceStub
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.canton.ledger.api.domain.LedgerId
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

final class ActiveContractSetClient(
    ledgerId: LedgerId,
    service: ActiveContractsServiceStub,
)(implicit
    esf: ExecutionSequencerFactory
) {
  private val it = new withoutledgerid.ActiveContractSetClient(service)

  /** Returns a stream of GetActiveContractsResponse messages. The materialized value will
    * be resolved to the offset that can be used as a starting offset for streaming transactions
    * via the transaction service.
    * If the stream completes before the offset can be set, the materialized future will
    * be failed with an exception.
    */
  def getActiveContracts(
      filter: TransactionFilter,
      verbose: Boolean = false,
      token: Option[String] = None,
  ): Source[GetActiveContractsResponse, Future[String]] =
    it.getActiveContracts(filter, ledgerId, verbose = verbose, token = token)
}
