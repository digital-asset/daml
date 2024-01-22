// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.state

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse.ContractEntry
import com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateServiceStub
import com.daml.ledger.api.v2.state_service.{
  ActiveContract,
  GetActiveContractsRequest,
  GetActiveContractsResponse,
  GetLedgerEndRequest,
}
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.util.pekkostreams.ExtractMaterializedValue
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
  ): Source[GetActiveContractsResponse, Future[String]] = {
    ClientAdapter
      .serverStreaming(
        GetActiveContractsRequest(
          filter = Some(filter),
          verbose = verbose,
          activeAtOffset = validAtOffset.getOrElse(""),
        ),
        LedgerClient.stub(service, token).getActiveContracts,
      )
      .viaMat(StateServiceClient.extractOffset)(
        Keep.right
      )
  }

  /** Returns the resulting active contract set */
  def getActiveContracts(
      filter: TransactionFilter,
      verbose: Boolean = false,
      validAtOffset: Option[String] = None,
      token: Option[String] = None,
  )(implicit
      materializer: Materializer
  ): Future[(Seq[ActiveContract], ParticipantOffset)] = {
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
    } yield (active, ParticipantOffset(value = ParticipantOffset.Value.Absolute(offset)))
  }

  /** Get the current participant offset */
  def getLedgerEnd(token: Option[String] = None): Future[ParticipantOffset] = {
    LedgerClient.stub(service, token).getLedgerEnd(GetLedgerEndRequest()).map { response =>
      response.offset.getOrElse(
        throw new IllegalStateException("Invalid empty getLedgerEnd response from server")
      )
    }
  }

}

object StateServiceClient {
  private val extractOffset =
    new ExtractMaterializedValue[GetActiveContractsResponse, String](r =>
      if (r.offset.nonEmpty) Some(r.offset) else None
    )

}
