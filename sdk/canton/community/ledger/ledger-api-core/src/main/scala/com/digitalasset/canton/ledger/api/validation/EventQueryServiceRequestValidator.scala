// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.event_query_service.{
  GetEventsByContractIdRequest,
  GetEventsByContractKeyRequest,
}
import com.daml.lf.data.Ref
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.digitalasset.canton.ledger.api.messages.event
import com.digitalasset.canton.ledger.api.messages.event.KeyContinuationToken
import com.digitalasset.canton.ledger.api.validation.EventQueryServiceRequestValidator.KeyTypeValidator
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.{
  invalidArgument,
  invalidField,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import io.grpc.StatusRuntimeException

import scala.concurrent.{ExecutionContext, Future}

object EventQueryServiceRequestValidator {
  type Result[X] = Either[StatusRuntimeException, X]
  type KeyTypeValidator =
    (Ref.TypeConRef, Value, LoggingContextWithTrace) => Future[Either[String, GlobalKey]]
}

class EventQueryServiceRequestValidator(
    partyNameChecker: PartyNameChecker,
    keyTypeValidator: KeyTypeValidator,
) {

  import EventQueryServiceRequestValidator.Result

  private val partyValidator = new PartyValidator(partyNameChecker)

  import FieldValidator.*

  def validateEventsByContractId(
      req: GetEventsByContractIdRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[event.GetEventsByContractIdRequest] =
    for {
      contractId <- requireContractId(req.contractId, "contract_id")
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      parties <- partyValidator.requireKnownParties(req.requestingParties)
    } yield {
      event.GetEventsByContractIdRequest(contractId, parties)
    }

  def validateEventsByContractKey(
      req: GetEventsByContractKeyRequest
  )(implicit
      executionContext: ExecutionContext,
      contextualizedErrorLogger: ContextualizedErrorLogger,
      loggingContextWithTrace: LoggingContextWithTrace,
  ): Future[Result[event.GetEventsByContractKeyRequest]] =
    (for {
      apiContractKey <- requirePresence(req.contractKey, "contract_key")
      untypedKey <- ValueValidator.validateValue(apiContractKey)
      apiTemplateId <- requirePresence(req.templateId, "template_id")
      typeConRef <- FieldValidator.validateTypeConRef(apiTemplateId)
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      requestingParties <- partyValidator.requireKnownParties(req.requestingParties)
      token <- KeyContinuationToken.fromTokenString(req.continuationToken).left.map { err =>
        invalidField("continuation_token", err)
      }
    } yield {
      (untypedKey, typeConRef, requestingParties, token)
    }) match {
      case Right((untypedKey, typeConRef, requestingParties, token)) =>
        keyTypeValidator(typeConRef, untypedKey, loggingContextWithTrace).map {
          case Right(globalKey) =>
            Right(
              event.GetEventsByContractKeyRequest(
                globalKey = globalKey,
                requestingParties = requestingParties,
                keyContinuationToken = token,
              )
            )
          case Left(err) =>
            Left(invalidArgument(s"contract_key: $err"))
        }
      case Left(err) => Future.successful[Result[event.GetEventsByContractKeyRequest]](Left(err))
    }

}
