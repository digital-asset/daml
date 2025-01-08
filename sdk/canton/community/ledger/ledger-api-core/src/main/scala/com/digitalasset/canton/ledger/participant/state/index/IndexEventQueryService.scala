// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v2.event_query_service.EventQueryServiceGrpc.EventQueryService]]
  */
trait IndexEventQueryService extends LedgerEndService {

  def getEventsByContractId(
      contractId: ContractId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractIdResponse]

  // TODO(i16065): Re-enable getEventsByContractKey tests
//  def getEventsByContractKey(
//      contractKey: Value,
//      templateId: Ref.Identifier,
//      requestingParties: Set[Ref.Party],
//      endExclusiveSeqId: Option[Long],
//  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractKeyResponse]

}
