// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.api.v1.event_query_service.{
  GetEventsByContractIdResponse,
  GetEventsByContractKeyResponse,
}
import com.daml.lf.data.Ref
import com.daml.lf.ledger.EventId
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v1.event_query_service.EventQueryServiceGrpc.EventQueryService]]
  */
trait IndexEventQueryService extends LedgerEndService {

  def getEventsByContractId(
      contractId: ContractId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[GetEventsByContractIdResponse]

  def getEventsByContractKey(
      contractKey: Value,
      templateId: Ref.Identifier,
      requestingParties: Set[Ref.Party],
      endExclusiveEventId: Option[EventId],
  )(implicit loggingContext: LoggingContext): Future[GetEventsByContractKeyResponse]

}
