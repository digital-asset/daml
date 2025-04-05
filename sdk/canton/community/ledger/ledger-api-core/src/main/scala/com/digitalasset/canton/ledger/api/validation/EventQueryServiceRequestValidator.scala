// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdRequest
import com.digitalasset.canton.ledger.api.messages.event
import com.digitalasset.canton.ledger.api.{CumulativeFilter, EventFormat, TemplateWildcardFilter}
import com.digitalasset.canton.logging.ContextualizedErrorLogger
import io.grpc.StatusRuntimeException

object EventQueryServiceRequestValidator {
  type Result[X] = Either[StatusRuntimeException, X]

  import FieldValidator.*
  import ValidationErrors.*

  def validateEventsByContractId(
      req: GetEventsByContractIdRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[event.GetEventsByContractIdRequest] =
    for {
      contractId <- requireContractId(req.contractId, "contract_id")
      parties <- requireParties(req.requestingParties.toSet)
      eventFormat <- req.eventFormat match {
        case None if parties.isEmpty =>
          Left(invalidArgument("Either event_format or requesting_parties needs to be defined."))

        case None =>
          Right(
            EventFormat(
              filtersByParty = parties.view
                .map(
                  _ -> CumulativeFilter(
                    templateFilters = Set.empty,
                    interfaceFilters = Set.empty,
                    templateWildcardFilter =
                      Some(TemplateWildcardFilter(includeCreatedEventBlob = false)),
                  )
                )
                .toMap,
              filtersForAnyParty = None,
              verbose = true,
            )
          )

        case Some(_) if parties.nonEmpty =>
          Left(
            invalidArgument(
              "Either event_format or requesting_parties needs to be defined, but not both."
            )
          )

        case Some(protoEventFormat) =>
          FormatValidator.validate(protoEventFormat)
      }

    } yield {
      event.GetEventsByContractIdRequest(contractId, eventFormat)
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
