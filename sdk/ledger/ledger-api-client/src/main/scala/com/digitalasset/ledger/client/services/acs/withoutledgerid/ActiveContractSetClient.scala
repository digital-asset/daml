// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.acs.withoutledgerid

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Source}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.akka.ClientAdapter
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsServiceStub
import com.daml.ledger.api.v1.active_contracts_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse,
}
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.client.LedgerClient
import com.daml.util.akkastreams.ExtractMaterializedValue
import scalaz.syntax.tag._

import scala.concurrent.Future

object ActiveContractSetClient {

  private val extractOffset =
    new ExtractMaterializedValue[GetActiveContractsResponse, String](r =>
      if (r.offset.nonEmpty) Some(r.offset) else None
    )

}

private[daml] final class ActiveContractSetClient(service: ActiveContractsServiceStub)(implicit
    esf: ExecutionSequencerFactory
) {

  import ActiveContractSetClient.extractOffset

  private def request(filter: TransactionFilter, verbose: Boolean, ledgerIdToUse: LedgerId) =
    GetActiveContractsRequest(
      ledgerIdToUse.unwrap,
      Some(filter),
      verbose,
    )

  private def activeContractSource(
      request: GetActiveContractsRequest,
      token: Option[String],
  ): Source[GetActiveContractsResponse, NotUsed] =
    ClientAdapter.serverStreaming(
      request,
      LedgerClient.stub(service, token).getActiveContracts,
    )

  /** Returns a stream of GetActiveContractsResponse messages. The materialized value will
    * be resolved to the offset that can be used as a starting offset for streaming transactions
    * via the transaction service.
    * If the stream completes before the offset can be set, the materialized future will
    * be failed with an exception.
    */
  def getActiveContracts(
      filter: TransactionFilter,
      ledgerIdToUse: LedgerId,
      verbose: Boolean = false,
      token: Option[String] = None,
  ): Source[GetActiveContractsResponse, Future[String]] =
    activeContractSource(request(filter, verbose, ledgerIdToUse), token).viaMat(extractOffset)(
      Keep.right
    )
}
