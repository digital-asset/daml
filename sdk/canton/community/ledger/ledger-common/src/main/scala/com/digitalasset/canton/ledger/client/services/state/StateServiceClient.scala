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
import com.digitalasset.canton.pekkostreams.ExtractMaterializedValue
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}

import scala.concurrent.{ExecutionContext, Future}

class StateServiceClient(service: StateServiceStub)(implicit
    ec: ExecutionContext,
    esf: ExecutionSequencerFactory,
) {

  /** Returns a stream of GetActiveContractsResponse messages. The materialized value will
    * be resolved to the offset that can be used as a starting offset for streaming transactions
    * via the transaction service.
    * If the stream completes before the offset can be set, the materialized future will
    * be failed with an exception.
    */
  def getActiveContractsSource(
      filter: TransactionFilter,
      verbose: Boolean = false,
      validAtOffset: Option[String] = None,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Source[GetActiveContractsResponse, Future[String]] =
    ClientAdapter
      .serverStreaming(
        GetActiveContractsRequest(
          filter = Some(filter),
          verbose = verbose,
          activeAtOffset = validAtOffset.getOrElse(""),
        ),
        LedgerClient.stubWithTracing(service, token).getActiveContracts,
      )
      .viaMat(StateServiceClient.extractOffset)(
        Keep.right
      )

  /** Returns the resulting active contract set */
  def getActiveContracts(
      filter: TransactionFilter,
      verbose: Boolean = false,
      validAtOffset: Option[String] = None,
      token: Option[String] = None,
  )(implicit
      materializer: Materializer,
      traceContext: TraceContext,
  ): Future[(Seq[ActiveContract], String)] = {
    val (offsetF, contractsF) =
      getActiveContractsSource(filter, verbose, validAtOffset, token)
        .toMat(Sink.seq)(Keep.both)
        .run()
    val activeF = contractsF
      .map(
        _.map(_.contractEntry)
          .collect { case ContractEntry.ActiveContract(value) =>
            value
          }
      )
    for {
      active <- activeF
      offset <- offsetF
    } yield (active, offset)
  }

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

object StateServiceClient {
  private val extractOffset =
    new ExtractMaterializedValue[GetActiveContractsResponse, String](r =>
      if (r.offset.nonEmpty) Some(r.offset) else None
    )

}
