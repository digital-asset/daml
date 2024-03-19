// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdRequest
import com.digitalasset.canton.ledger.api.messages.event
import io.grpc.StatusRuntimeException

object EventQueryServiceRequestValidator {
  type Result[X] = Either[StatusRuntimeException, X]

}
class EventQueryServiceRequestValidator(partyNameChecker: PartyNameChecker) {

  import EventQueryServiceRequestValidator.Result

  private val partyValidator = new PartyValidator(partyNameChecker)

  import FieldValidator.*

  def validateEventsByContractId(
      req: GetEventsByContractIdRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[event.GetEventsByContractIdRequest] = {
    for {
      contractId <- requireContractId(req.contractId, "contract_id")
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      parties <- partyValidator.requireKnownParties(req.requestingParties)
    } yield {
      event.GetEventsByContractIdRequest(contractId, parties)
    }
  }

  // TODO(i16065): Re-enable getEventsByContractKey tests
//  def validateEventsByContractKey(
//      req: GetEventsByContractKeyRequest
//  )(implicit
//      contextualizedErrorLogger: ContextualizedErrorLogger
//  ): Result[event.GetEventsByContractKeyRequest] = {
//
//    for {
//      apiContractKey <- requirePresence(req.contractKey, "contract_key")
//      contractKey <- ValueValidator.validateValue(apiContractKey)
//      apiTemplateId <- requirePresence(req.templateId, "template_id")
//      templateId <- validateIdentifier(apiTemplateId)
//      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
//      requestingParties <- partyValidator.requireKnownParties(req.requestingParties)
//      endExclusiveSeqId <- optionalEventSequentialId(
//        req.continuationToken,
//        "continuation_token",
//        "Invalid token", // Don't mention event sequential id as opaque
//      )
//    } yield {
//
//      event.GetEventsByContractKeyRequest(
//        contractKey = contractKey,
//        templateId = templateId,
//        requestingParties = requestingParties,
//        endExclusiveSeqId = endExclusiveSeqId,
//      )
//    }
//
//  }

}
