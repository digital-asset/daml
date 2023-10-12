// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.services

import com.daml.ledger.api.v1.event_query_service.{
  GetEventsByContractIdResponse,
  GetEventsByContractKeyResponse,
}
import com.digitalasset.canton.ledger.api.messages.event.{
  GetEventsByContractIdRequest,
  GetEventsByContractKeyRequest,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

trait EventQueryService {
  def getEventsByContractId(
      req: GetEventsByContractIdRequest
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractIdResponse]

  def getEventsByContractKey(
      req: GetEventsByContractKeyRequest
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractKeyResponse]

}
