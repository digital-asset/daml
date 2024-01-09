// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v1.value.Value
import com.daml.ledger.api.v1.{event_query_service, value as api}
import com.daml.lf.value.{Value as Lf}
import com.digitalasset.canton.ledger.api.messages.event
import io.grpc.Status.Code.*
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

class EventQueryServiceRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with MockitoSugar {

  private implicit val noLogging: ContextualizedErrorLogger = NoLogging

  private val validator = new EventQueryServiceRequestValidator(PartyNameChecker.AllowAllParties)

  "EventQueryServiceRequestValidator" when {

    "validating event by contract id requests" should {

      val expected = event.GetEventsByContractIdRequest(
        contractId = contractId,
        requestingParties = Set(party),
      )

      val req = event_query_service.GetEventsByContractIdRequest(
        contractId.coid,
        expected.requestingParties.toSeq,
      )

      "pass on valid input" in {
        validator.validateEventsByContractId(req) shouldBe Right(expected)
      }

      "fail on empty contractId" in {
        requestMustFailWith(
          request = validator.validateEventsByContractId(req.withContractId("")),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: contract_id",
          metadata = Map.empty,
        )
      }

      "fail on empty requesting parties" in {
        requestMustFailWith(
          request = validator.validateEventsByContractId(req.withRequestingParties(Nil)),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
          metadata = Map.empty,
        )
      }

    }

    "validating event by contract key requests" should {

      val txRequest = event.GetEventsByContractKeyRequest(
        contractKey = Lf.ValueText("contractKey"),
        templateId = refTemplateId,
        requestingParties = Set(party),
        endExclusiveSeqId = None,
      )

      val apiRequest = event_query_service.GetEventsByContractKeyRequest(
        contractKey = Some(api.Value(Value.Sum.Text("contractKey"))),
        templateId = Some(
          com.daml.ledger.api.v1.value
            .Identifier(packageId, moduleName.toString, dottedName.toString)
        ),
        requestingParties = txRequest.requestingParties.toSeq,
      )

      "pass on valid input" in {
        validator.validateEventsByContractKey(apiRequest) shouldBe Right(txRequest)
      }

      "fail on empty contract_key" in {
        requestMustFailWith(
          request = validator.validateEventsByContractKey(apiRequest.clearContractKey),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: contract_key",
          metadata = Map.empty,
        )
      }

      "fail on empty template_id" in {
        requestMustFailWith(
          request = validator.validateEventsByContractKey(apiRequest.clearTemplateId),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: template_id",
          metadata = Map.empty,
        )
      }

      "fail on empty requesting_parties" in {
        requestMustFailWith(
          request = validator.validateEventsByContractKey(apiRequest.withRequestingParties(Nil)),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
          metadata = Map.empty,
        )
      }

    }

  }
}
