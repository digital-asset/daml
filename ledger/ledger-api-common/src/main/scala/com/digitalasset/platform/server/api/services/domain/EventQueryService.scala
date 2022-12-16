// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.domain

import com.daml.ledger.api.messages.event.{
  GetEventsByContractIdRequest,
  GetEventsByContractKeyRequest,
}
import com.daml.ledger.api.v1.event_query_service.{
  GetEventsByContractIdResponse,
  GetEventsByContractKeyResponse,
}
import com.daml.logging.LoggingContext

import scala.concurrent.Future

trait EventQueryService {
  def getEventsByContractId(
      req: GetEventsByContractIdRequest
  )(implicit loggingContext: LoggingContext): Future[GetEventsByContractIdResponse]

  def getEventsByContractKey(
      req: GetEventsByContractKeyRequest
  )(implicit loggingContext: LoggingContext): Future[GetEventsByContractKeyResponse]

}
