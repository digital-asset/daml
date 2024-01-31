// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import com.daml.ledger.api.v1.event_query_service.GetEventsByContractKeyResponse
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.lf.data.Ref
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v1.event_query_service.EventQueryServiceGrpc.EventQueryService]]
  */
trait IndexEventQueryService extends LedgerEndService {

  def getEventsByContractId(
      contractId: ContractId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractIdResponse]

  def getEventsByContractKey(
      contractKey: Value,
      templateId: Ref.Identifier,
      requestingParties: Set[Ref.Party],
      endExclusiveSeqId: Option[Long],
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractKeyResponse]

}
