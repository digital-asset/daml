// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  GetConnectedSynchronizersRequest,
  GetConnectedSynchronizersResponse,
  GetLedgerEndRequest,
  GetLedgerEndResponse,
}
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, TransactionFilter}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import scala.concurrent.{ExecutionContext, Future}

class StateServiceClient(
    service: StateServiceStub,
    getDefaultToken: () => Option[String] = () => None,
)(implicit
    ec: ExecutionContext,
    esf: ExecutionSequencerFactory,
) {

  /** Returns a stream of GetActiveContractsResponse messages. */
  // TODO(#23504) remove when TransactionFilter is removed
  @deprecated(
    "Use getActiveContractsSource with EventFormat instead",
    "3.4.0",
  )
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
          eventFormat = None,
        ),
        LedgerClient.stubWithTracing(service, token.orElse(getDefaultToken())).getActiveContracts,
      )

  /** Returns a stream of GetActiveContractsResponse messages. */
  def getActiveContractsSource(
      eventFormat: EventFormat,
      validAtOffset: Long,
      token: Option[String],
  )(implicit traceContext: TraceContext): Source[GetActiveContractsResponse, NotUsed] =
    ClientAdapter
      .serverStreaming(
        GetActiveContractsRequest(
          filter = None,
          verbose = false,
          activeAtOffset = validAtOffset,
          eventFormat = Some(eventFormat),
        ),
        LedgerClient.stubWithTracing(service, token.orElse(getDefaultToken())).getActiveContracts,
      )

  /** Returns the resulting active contract set */
  // TODO(#23504) remove when TransactionFilter is removed
  @deprecated(
    "Use getActiveContracts with EventFormat instead",
    "3.4.0",
  )
  def getActiveContracts(
      filter: TransactionFilter,
      validAtOffset: Long,
      verbose: Boolean,
      token: Option[String],
  )(implicit
      materializer: Materializer,
      traceContext: TraceContext,
  ): Future[Seq[ActiveContract]] =
    for {
      contracts <- getActiveContractsSource(filter, validAtOffset, verbose, token).runWith(Sink.seq)
      active = contracts
        .map(_.contractEntry)
        .collect { case ContractEntry.ActiveContract(value) =>
          value
        }
    } yield active

  /** Returns the resulting active contract set */
  def getActiveContracts(
      eventFormat: EventFormat,
      validAtOffset: Long,
      token: Option[String] = None,
  )(implicit
      materializer: Materializer,
      traceContext: TraceContext,
  ): Future[Seq[ActiveContract]] =
    for {
      contracts <- getActiveContractsSource(eventFormat, validAtOffset, token).runWith(Sink.seq)
      active = contracts
        .map(_.contractEntry)
        .collect { case ContractEntry.ActiveContract(value) =>
          value
        }
    } yield active

  def getLedgerEnd(
      token: Option[String] = None
  )(implicit traceContext: TraceContext): Future[GetLedgerEndResponse] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .getLedgerEnd(GetLedgerEndRequest())

  /** Get the current participant offset */
  def getLedgerEndOffset(
      token: Option[String] = None
  )(implicit traceContext: TraceContext): Future[Long] =
    getLedgerEnd(token).map(_.offset)

  def getConnectedSynchronizers(
      party: String,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[GetConnectedSynchronizersResponse] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .getConnectedSynchronizers(
        GetConnectedSynchronizersRequest(
          party = party,
          participantId = "",
          identityProviderId = "",
        )
      )
}
