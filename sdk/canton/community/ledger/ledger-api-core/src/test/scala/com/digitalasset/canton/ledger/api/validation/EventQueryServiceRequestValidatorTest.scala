// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.event_query_service
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{Filters, WildcardFilter}
import com.digitalasset.canton.ledger.api.messages.event
import com.digitalasset.canton.ledger.api.{CumulativeFilter, EventFormat, TemplateWildcardFilter}
import com.digitalasset.canton.logging.{ContextualizedErrorLogger, NoLogging}
import io.grpc.Status.Code.*
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

class EventQueryServiceRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with MockitoSugar {

  private implicit val noLogging: ContextualizedErrorLogger = NoLogging

  "EventQueryServiceRequestValidator" when {

    val someProtoEventFormat = com.daml.ledger.api.v2.transaction_filter.EventFormat(
      filtersByParty = Map(
        party.toString -> Filters(
          Seq(
            com.daml.ledger.api.v2.transaction_filter.CumulativeFilter(
              IdentifierFilter.WildcardFilter(WildcardFilter(true))
            )
          )
        )
      ),
      filtersForAnyParty = None,
      verbose = false,
    )

    // TODO(i23504): remove
    "validating legacy event by contract id requests" should {

      val expected = event.GetEventsByContractIdRequest(
        contractId = contractId,
        eventFormat = EventFormat(
          filtersByParty = Map(
            party -> CumulativeFilter(
              templateFilters = Set.empty,
              interfaceFilters = Set.empty,
              templateWildcardFilter = Some(TemplateWildcardFilter(includeCreatedEventBlob = false)),
            )
          ),
          filtersForAnyParty = None,
          verbose = true,
        ),
      )

      val req = event_query_service.GetEventsByContractIdRequest(
        contractId.coid,
        Seq(party),
        None,
      )

      "pass on valid input" in {
        EventQueryServiceRequestValidator.validateEventsByContractId(req) shouldBe Right(expected)
      }

      "fail on empty contractId" in {
        requestMustFailWith(
          request =
            EventQueryServiceRequestValidator.validateEventsByContractId(req.withContractId("")),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: contract_id",
          metadata = Map.empty,
        )
      }

      "fail on empty requesting parties" in {
        requestMustFailWith(
          request = EventQueryServiceRequestValidator.validateEventsByContractId(
            req.withRequestingParties(Nil)
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Either event_format or requesting_parties needs to be defined.",
          metadata = Map.empty,
        )
      }

      "fail if event format is also defined" in {
        requestMustFailWith(
          request = EventQueryServiceRequestValidator.validateEventsByContractId(
            req.withEventFormat(someProtoEventFormat)
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Either event_format or requesting_parties needs to be defined, but not both.",
          metadata = Map.empty,
        )
      }
    }

    "validating event by contract id requests" should {

      val expected = event.GetEventsByContractIdRequest(
        contractId = contractId,
        eventFormat = EventFormat(
          filtersByParty = Map(
            party -> CumulativeFilter(
              templateFilters = Set.empty,
              interfaceFilters = Set.empty,
              templateWildcardFilter = Some(TemplateWildcardFilter(includeCreatedEventBlob = true)),
            )
          ),
          filtersForAnyParty = None,
          verbose = false,
        ),
      )

      val req = event_query_service.GetEventsByContractIdRequest(
        contractId.coid,
        Nil,
        Some(someProtoEventFormat),
      )

      "pass on valid input" in {
        EventQueryServiceRequestValidator.validateEventsByContractId(req) shouldBe Right(expected)
      }

      "fail on empty contractId" in {
        requestMustFailWith(
          request =
            EventQueryServiceRequestValidator.validateEventsByContractId(req.withContractId("")),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: contract_id",
          metadata = Map.empty,
        )
      }

      "fail on empty event format" in {
        requestMustFailWith(
          request = EventQueryServiceRequestValidator.validateEventsByContractId(
            req.clearEventFormat
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Either event_format or requesting_parties needs to be defined.",
          metadata = Map.empty,
        )
      }
    }

    // TODO(i16065): Re-enable getEventsByContractKey tests
//    "validating event by contract key requests" should {
//
//      val txRequest = event.GetEventsByContractKeyRequest(
//        contractKey = Lf.ValueText("contractKey"),
//        templateId = refTemplateId,
//        requestingParties = Set(party),
//        endExclusiveSeqId = None,
//      )
//
//      val apiRequest = event_query_service.GetEventsByContractKeyRequest(
//        contractKey = Some(api.Value(Value.Sum.Text("contractKey"))),
//        templateId = Some(
//          com.daml.ledger.api.v2.value
//            .Identifier(packageId, moduleName.toString, dottedName.toString)
//        ),
//        requestingParties = txRequest.requestingParties.toSeq,
//      )
//
//      "pass on valid input" in  {
//        validator.validateEventsByContractKey(apiRequest) shouldBe Right(txRequest)
//      }
//
//
//      "fail on empty contract_key" in {
//        requestMustFailWith(
//          request = validator.validateEventsByContractKey(apiRequest.clearContractKey),
//          code = INVALID_ARGUMENT,
//          description =
//            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: contract_key",
//          metadata = Map.empty,
//        )
//      }
//
//      "fail on empty template_id" in {
//        requestMustFailWith(
//          request = validator.validateEventsByContractKey(apiRequest.clearTemplateId),
//          code = INVALID_ARGUMENT,
//          description =
//            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: template_id",
//          metadata = Map.empty,
//        )
//      }
//
//      "fail on empty requesting_parties" in  {
//        requestMustFailWith(
//          request = validator.validateEventsByContractKey(apiRequest.withRequestingParties(Nil)),
//          code = INVALID_ARGUMENT,
//          description =
//            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
//          metadata = Map.empty,
//        )
//      }
//
//    }

  }
}
