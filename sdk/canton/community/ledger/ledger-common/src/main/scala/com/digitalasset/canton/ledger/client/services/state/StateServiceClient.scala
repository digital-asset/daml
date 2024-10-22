// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.state

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse.ContractEntry
import com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateServiceStub
import com.daml.ledger.api.v2.state_service.{
  ActiveContract,
  GetActiveContractsRequest,
  GetActiveContractsResponse,
  GetLedgerEndRequest,
  GetLedgerEndResponse,
}
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import scala.concurrent.{ExecutionContext, Future}

class StateServiceClient(service: StateServiceStub)(implicit
    ec: ExecutionContext,
    esf: ExecutionSequencerFactory,
) {

  /** Returns a stream of GetActiveContractsResponse messages. */
  def getActiveContractsSource(
      filter: TransactionFilter,
      validAtOffset: Long,
      verbose: Boolean = false,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Source[GetActiveContractsResponse, NotUsed] =
    ClientAdapter
      .serverStreaming(
        GetActiveContractsRequest(
          filter = Some(filter),
          verbose = verbose,
          activeAtOffset = validAtOffset,
        ),
        LedgerClient.stubWithTracing(service, token).getActiveContracts,
      )

  /** Returns the resulting active contract set */
  def getActiveContracts(
      filter: TransactionFilter,
      validAtOffset: Long,
      verbose: Boolean = false,
      token: Option[String] = None,
  )(implicit
      materializer: Materializer,
      traceContext: TraceContext,
  ): Future[(Seq[ActiveContract], Long)] =
    for {
      contracts <- getActiveContractsSource(filter, validAtOffset, verbose, token).runWith(Sink.seq)
      active = contracts
        .map(_.contractEntry)
        .collect { case ContractEntry.ActiveContract(value) =>
          value
        }
    } yield (active, validAtOffset)

  def getLedgerEnd(
      token: Option[String] = None
  )(implicit traceContext: TraceContext): Future[GetLedgerEndResponse] =
    LedgerClient
      .stubWithTracing(service, token)
      .getLedgerEnd(GetLedgerEndRequest())

  /** Get the current participant offset */
  def getLedgerEndOffset(
      token: Option[String] = None
  )(implicit traceContext: TraceContext): Future[Option[Long]] =
    getLedgerEnd(token).map { response =>
      response.offset
    }

}
