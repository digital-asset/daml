// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.updates

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  TransactionFilter,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.api.v2.update_service.UpdateServiceGrpc.UpdateServiceStub
import com.daml.ledger.api.v2.update_service.{GetUpdatesRequest, GetUpdatesResponse}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

class UpdateServiceClient(
    service: UpdateServiceStub,
    getDefaultToken: () => Option[String] = () => None,
)(implicit
    esf: ExecutionSequencerFactory
) {
  // TODO(#23504) remove when TransactionFilter is removed
  @deprecated(
    "Use getUpdatesSource with EventFormat instead",
    "3.4.0",
  )
  def getUpdatesSource(
      begin: Long,
      filter: TransactionFilter,
      verbose: Boolean,
      end: Option[Long],
      token: Option[String],
  )(implicit traceContext: TraceContext): Source[GetUpdatesResponse, NotUsed] =
    ClientAdapter
      .serverStreaming(
        GetUpdatesRequest(
          beginExclusive = begin,
          endInclusive = end,
          filter = Some(filter),
          verbose = verbose,
          updateFormat = None,
        ),
        LedgerClient.stubWithTracing(service, token.orElse(getDefaultToken())).getUpdates,
      )

  def getUpdatesSource(
      begin: Long,
      eventFormat: EventFormat,
      end: Option[Long] = None,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Source[GetUpdatesResponse, NotUsed] =
    ClientAdapter
      .serverStreaming(
        GetUpdatesRequest(
          beginExclusive = begin,
          endInclusive = end,
          filter = None,
          verbose = false,
          updateFormat = Some(
            UpdateFormat(
              includeTransactions = Some(
                TransactionFormat(
                  eventFormat = Some(eventFormat),
                  transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
                )
              ),
              includeReassignments = Some(eventFormat),
              includeTopologyEvents = None,
            )
          ),
        ),
        LedgerClient.stubWithTracing(service, token.orElse(getDefaultToken())).getUpdates,
      )
}
