// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services

import com.daml.ledger.api.v1.event_query_service.EventQueryServiceGrpc.EventQueryServiceStub
import com.daml.ledger.api.v1.event_query_service.{
  GetEventsByContractIdRequest,
  GetEventsByContractIdResponse,
  GetEventsByContractKeyRequest,
  GetEventsByContractKeyResponse,
}
import com.daml.ledger.api.v1.value.Identifier
import com.digitalasset.canton.ledger.client.LedgerClient

import scala.concurrent.Future

class EventQueryServiceClient(service: EventQueryServiceStub) {
  def getEventsByContractId(
      contractId: String,
      requestingParties: Seq[String],
      token: Option[String] = None,
  ): Future[GetEventsByContractIdResponse] =
    LedgerClient
      .stub(service, token)
      .getEventsByContractId(
        GetEventsByContractIdRequest(
          contractId = contractId,
          requestingParties = requestingParties,
        )
      )

  def getEventsByContractKey(
      contractKey: com.daml.ledger.api.v1.value.Value,
      templateId: Identifier,
      requestingParties: Seq[String],
      continuationToken: String,
      token: Option[String] = None,
  ): Future[GetEventsByContractKeyResponse] =
    LedgerClient
      .stub(service, token)
      .getEventsByContractKey(
        GetEventsByContractKeyRequest(
          contractKey = Some(contractKey),
          templateId = Some(templateId),
          requestingParties = requestingParties,
          continuationToken = continuationToken,
        )
      )
}
