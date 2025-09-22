// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services

import com.daml.ledger.api.v2.event_query_service.EventQueryServiceGrpc.EventQueryServiceStub
import com.daml.ledger.api.v2.event_query_service.{
  GetEventsByContractIdRequest,
  GetEventsByContractIdResponse,
}
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters}
import com.digitalasset.canton.ledger.client.LedgerClient

import scala.concurrent.Future

class EventQueryServiceClient(
    service: EventQueryServiceStub,
    getDefaultToken: () => Option[String] = () => None,
) {
  def getEventsByContractId(
      contractId: String,
      requestingParties: Seq[String],
      token: Option[String] = None,
  ): Future[GetEventsByContractIdResponse] = {
    val eventFormat = EventFormat(
      filtersByParty = requestingParties.map(_ -> Filters(Nil)).toMap,
      filtersForAnyParty = None,
      verbose = true,
    )

    LedgerClient
      .stub(service, token.orElse(getDefaultToken()))
      .getEventsByContractId(
        GetEventsByContractIdRequest(
          contractId = contractId,
          eventFormat = Some(eventFormat),
        )
      )
  }

//  TODO(#16065)
//  def getEventsByContractKey(
//      contractKey: com.daml.ledger.api.v2.value.Value,
//      templateId: Identifier,
//      requestingParties: Seq[String],
//      continuationToken: String,
//      token: Option[String] = None,
//  ): Future[GetEventsByContractKeyResponse] =
//    LedgerClient
//      .stub(service, token.orElse(getDefaultToken())))
//      .getEventsByContractKey(
//        GetEventsByContractKeyRequest(
//          contractKey = Some(contractKey),
//          templateId = Some(templateId),
//          requestingParties = requestingParties,
//          continuationToken = continuationToken,
//        )
//      )
}
