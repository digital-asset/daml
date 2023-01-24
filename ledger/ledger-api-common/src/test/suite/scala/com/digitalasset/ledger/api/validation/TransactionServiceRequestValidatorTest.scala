// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  InterfaceFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v1.transaction_service.{
  GetEventsByContractIdRequest,
  GetEventsByContractKeyRequest,
  GetLedgerEndRequest,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest,
}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.lf.value.Value.ValueText
import io.grpc.Status.Code._
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec
import com.daml.ledger.api.v1.{value => api}
import com.daml.lf.value.{Value => Lf}

class TransactionServiceRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with MockitoSugar {
  private implicit val noLogging: ContextualizedErrorLogger = NoLogging

  private val txReq = GetTransactionsRequest(
    expectedLedgerId,
    Some(LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.LEDGER_BEGIN))),
    Some(LedgerOffset(LedgerOffset.Value.Absolute(absoluteOffset))),
    Some(
      TransactionFilter(
        Map(
          party ->
            Filters(
              Some(
                InclusiveFilters(
                  Seq(
                    Identifier(
                      packageId,
                      moduleName = includedModule,
                      entityName = includedTemplate,
                    )
                  ),
                  interfaceFilters = Seq(
                    InterfaceFilter(
                      interfaceId = Some(
                        Identifier(
                          packageId,
                          moduleName = includedModule,
                          entityName = includedTemplate,
                        )
                      ),
                      includeInterfaceView = true,
                      includeCreateArgumentsBlob = true,
                    )
                  ),
                )
              )
            )
        )
      )
    ),
    verbose,
  )
  private val txTreeReq = GetTransactionsRequest(
    expectedLedgerId,
    Some(LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.LEDGER_BEGIN))),
    Some(LedgerOffset(LedgerOffset.Value.Absolute(absoluteOffset))),
    Some(TransactionFilter(Map(party -> Filters.defaultInstance))),
    verbose,
  )

  private val endReq = GetLedgerEndRequest(expectedLedgerId)

  private val txByEvIdReq =
    GetTransactionByEventIdRequest(expectedLedgerId, eventId, Seq(party))

  private val txByIdReq =
    GetTransactionByIdRequest(expectedLedgerId, transactionId, Seq(party))

  private val validator = new TransactionServiceRequestValidator(
    domain.LedgerId(expectedLedgerId),
    PartyNameChecker.AllowAllParties,
  )

  "TransactionRequestValidation" when {

    "validating regular requests" should {

      "accept requests with empty ledger ID" in {
        inside(validator.validate(txReq.withLedgerId(""), ledgerEnd)) { case Right(req) =>
          req.ledgerId shouldEqual None
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          val filtersByParty = req.filter.filtersByParty
          filtersByParty should have size 1
          hasExpectedFilters(req)
          req.verbose shouldEqual verbose
        }

      }

      "return the correct error on missing filter" in {
        requestMustFailWith(
          validator.validate(txReq.update(_.optionalFilter := None), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: filter",
          metadata = Map.empty,
        )
      }

      "return the correct error on empty filter" in {
        requestMustFailWith(
          request = validator.validate(
            txReq.update(_.filter.filtersByParty := Map.empty),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: filtersByParty cannot be empty",
          metadata = Map.empty,
        )
      }

      "return the correct error on empty interfaceId in interfaceFilter" in {
        requestMustFailWith(
          request = validator.validate(
            txReq.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.inclusive := InclusiveFilters(Nil, Seq(InterfaceFilter(None, true))))
            })),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: interfaceId",
          metadata = Map.empty,
        )
      }

      "return the correct error on missing begin" in {
        requestMustFailWith(
          request = validator.validate(txReq.update(_.optionalBegin := None), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: begin",
          metadata = Map.empty,
        )
      }

      "return the correct error on empty begin " in {
        requestMustFailWith(
          request = validator.validate(txReq.update(_.begin := LedgerOffset()), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: begin.(boundary|value)",
          metadata = Map.empty,
        )
      }

      "return the correct error on empty end " in {
        requestMustFailWith(
          request = validator.validate(txReq.withEnd(LedgerOffset()), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: end.(boundary|value)",
          metadata = Map.empty,
        )
      }

      "return the correct error on unknown begin boundary" in {
        requestMustFailWith(
          request = validator.validate(
            txReq.withBegin(
              LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.Unrecognized(7)))
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown ledger boundary value '7' in field begin.boundary",
          metadata = Map.empty,
        )
      }

      "return the correct error on unknown end boundary" in {
        requestMustFailWith(
          request = validator.validate(
            txReq.withEnd(
              LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.Unrecognized(7)))
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown ledger boundary value '7' in field end.boundary",
          metadata = Map.empty,
        )
      }

      "return the correct error when begin offset is after ledger end" in {
        requestMustFailWith(
          request = validator.validate(
            txReq.withBegin(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            "OFFSET_AFTER_LEDGER_END(12,0): Begin offset (1001) is after ledger end (1000)",
          metadata = Map.empty,
        )
      }

      "return the correct error when end offset is after ledger end" in {
        requestMustFailWith(
          request = validator.validate(
            txReq.withEnd(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            "OFFSET_AFTER_LEDGER_END(12,0): End offset (1001) is after ledger end (1000)",
          metadata = Map.empty,
        )
      }

      "tolerate missing end" in {
        inside(validator.validate(txReq.update(_.optionalEnd := None), ledgerEnd)) {
          case Right(req) =>
            req.ledgerId shouldEqual Some(expectedLedgerId)
            req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
            req.endInclusive shouldEqual None
            val filtersByParty = req.filter.filtersByParty
            filtersByParty should have size 1
            hasExpectedFilters(req)
            req.verbose shouldEqual verbose
        }
      }

      "tolerate empty filters_inclusive" in {
        inside(
          validator.validate(
            txReq.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.inclusive := InclusiveFilters(Nil, Nil))
            })),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.ledgerId shouldEqual Some(expectedLedgerId)
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          val filtersByParty = req.filter.filtersByParty
          filtersByParty should have size 1
          inside(filtersByParty.headOption.value) { case (p, filters) =>
            p shouldEqual party
            filters shouldEqual domain.Filters(Some(domain.InclusiveFilters(Set(), Set())))
          }
          req.verbose shouldEqual verbose
        }
      }

      "tolerate missing filters_inclusive" in {
        inside(
          validator.validate(
            txReq.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.optionalInclusive := None)
            })),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.ledgerId shouldEqual Some(expectedLedgerId)
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          val filtersByParty = req.filter.filtersByParty
          filtersByParty should have size 1
          inside(filtersByParty.headOption.value) { case (p, filters) =>
            p shouldEqual party
            filters shouldEqual domain.Filters(None)
          }
          req.verbose shouldEqual verbose
        }
      }

      "tolerate all fields filled out" in {
        inside(validator.validate(txReq, ledgerEnd)) { case Right(req) =>
          req.ledgerId shouldEqual Some(expectedLedgerId)
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          hasExpectedFilters(req)
          req.verbose shouldEqual verbose
        }
      }
    }

    "validating tree requests" should {

      "tolerate missing filters_inclusive" in {
        inside(validator.validateTree(txTreeReq, ledgerEnd)) { case Right(req) =>
          req.ledgerId shouldEqual Some(expectedLedgerId)
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          req.parties should have size 1
          req.parties.headOption.value shouldEqual party
          req.verbose shouldEqual verbose
        }
      }

      "not tolerate having filters_inclusive" in {
        requestMustFailWith(
          request = validator.validateTree(
            txTreeReq.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.optionalInclusive := Some(InclusiveFilters()))
            })),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: party attempted subscription for templates []. Template filtration is not supported on GetTransactionTrees RPC. To get filtered data, use the GetTransactions RPC.",
          metadata = Map.empty,
        )
      }

      "return the correct error when begin offset is after ledger end" in {
        requestMustFailWith(
          request = validator.validateTree(
            txTreeReq.withBegin(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            "OFFSET_AFTER_LEDGER_END(12,0): Begin offset (1001) is after ledger end (1000)",
          metadata = Map.empty,
        )
      }

      "return the correct error when end offset is after ledger end" in {
        requestMustFailWith(
          request = validator.validateTree(
            txTreeReq.withEnd(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            "OFFSET_AFTER_LEDGER_END(12,0): End offset (1001) is after ledger end (1000)",
          metadata = Map.empty,
        )
      }
    }

    "validating ledger end requests" should {

      "fail on ledger ID mismatch" in {
        requestMustFailWith(
          request = validator.validateLedgerEnd(endReq.withLedgerId("mismatchedLedgerId")),
          code = NOT_FOUND,
          description =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID 'mismatchedLedgerId' not found. Actual Ledger ID is 'expectedLedgerId'.",
          metadata = Map.empty,
        )
      }

      "succeed validating a correct request" in {
        inside(validator.validateLedgerEnd(endReq)) { case Right(_) =>
          succeed
        }
      }
    }

    "validating transaction by id requests" should {

      "fail on ledger ID mismatch" in {
        requestMustFailWith(
          request = validator.validateTransactionById(txByIdReq.withLedgerId("mismatchedLedgerId")),
          code = NOT_FOUND,
          description =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID 'mismatchedLedgerId' not found. Actual Ledger ID is 'expectedLedgerId'.",
          metadata = Map.empty,
        )
      }

      "fail on empty transactionId" in {
        requestMustFailWith(
          request = validator.validateTransactionById(txByIdReq.withTransactionId("")),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: transaction_id",
          metadata = Map.empty,
        )
      }

      "fail on empty requesting parties" in {
        requestMustFailWith(
          request = validator.validateTransactionById(txByIdReq.withRequestingParties(Nil)),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
          metadata = Map.empty,
        )
      }

      "return passed ledger ID" in {
        inside(validator.validateTransactionById(txByIdReq)) { case Right(out) =>
          out should have(Symbol("ledgerId")(Some(expectedLedgerId)))
        }
      }

    }

    "validating transaction by event id requests" should {

      "fail on ledger ID mismatch" in {
        requestMustFailWith(
          request =
            validator.validateTransactionByEventId(txByEvIdReq.withLedgerId("mismatchedLedgerId")),
          code = NOT_FOUND,
          description =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID 'mismatchedLedgerId' not found. Actual Ledger ID is 'expectedLedgerId'.",
          metadata = Map.empty,
        )
      }

      "fail on empty eventId" in {
        requestMustFailWith(
          request = validator.validateTransactionByEventId(txByEvIdReq.withEventId("")),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: event_id",
          metadata = Map.empty,
        )
      }

      "fail on empty requesting parties" in {
        requestMustFailWith(
          request = validator.validateTransactionByEventId(txByEvIdReq.withRequestingParties(Nil)),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
          metadata = Map.empty,
        )
      }

      "return passed ledger ID" in {
        inside(
          validator.validateTransactionByEventId(txByEvIdReq)
        ) { case Right(out) =>
          out should have(Symbol("ledgerId")(Some(expectedLedgerId)))
        }
      }

    }

    "applying party name checks" should {

      val partyRestrictiveValidator = new TransactionServiceRequestValidator(
        domain.LedgerId(expectedLedgerId),
        PartyNameChecker.AllowPartySet(Set(party)),
      )

      val partyWithUnknowns = List("party", "Alice", "Bob")
      val filterWithUnknown =
        TransactionFilter(partyWithUnknowns.map(_ -> Filters.defaultInstance).toMap)
      val filterWithKnown =
        TransactionFilter(Map(party -> Filters.defaultInstance))

      "reject transaction requests for unknown parties" in {
        requestMustFailWith(
          request =
            partyRestrictiveValidator.validate(txReq.withFilter(filterWithUnknown), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
          metadata = Map.empty,
        )
      }

      "reject transaction tree requests for unknown parties" in {
        requestMustFailWith(
          request = partyRestrictiveValidator
            .validateTree(txTreeReq.withFilter(filterWithUnknown), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
          metadata = Map.empty,
        )
      }

      "reject transaction by id requests for unknown parties" in {
        requestMustFailWith(
          request = partyRestrictiveValidator.validateTransactionById(
            txByIdReq.withRequestingParties(partyWithUnknowns)
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
          metadata = Map.empty,
        )
      }

      "reject transaction by event id requests for unknown parties" in {
        requestMustFailWith(
          request = partyRestrictiveValidator.validateTransactionById(
            txByIdReq.withRequestingParties(partyWithUnknowns)
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
          metadata = Map.empty,
        )
      }

      "accept transaction requests for known parties" in {
        partyRestrictiveValidator.validate(
          txReq.withFilter(filterWithKnown),
          ledgerEnd,
        ) shouldBe a[Right[_, _]]
      }

      "accept transaction tree requests for known parties" in {
        partyRestrictiveValidator.validateTree(
          txTreeReq.withFilter(filterWithKnown),
          ledgerEnd,
        ) shouldBe a[Right[_, _]]
      }

      "accept transaction by id requests for known parties" in {
        partyRestrictiveValidator.validateTransactionById(
          txByIdReq.withRequestingParties(List("party"))
        ) shouldBe a[Right[_, _]]
      }

      "accept transaction by event id requests for known parties" in {
        partyRestrictiveValidator.validateTransactionById(
          txByIdReq.withRequestingParties(List("party"))
        ) shouldBe a[Right[_, _]]
      }
    }

    "validating event by contract id requests" should {

      val expected = com.daml.ledger.api.messages.transaction.GetEventsByContractIdRequest(
        contractId = contractId,
        requestingParties = Set(party),
      )
      val req = GetEventsByContractIdRequest(contractId.coid, expected.requestingParties.toSeq)

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

      val txRequest = com.daml.ledger.api.messages.transaction.GetEventsByContractKeyRequest(
        contractKey = Lf.ValueText("contractKey"),
        templateId = templateId,
        requestingParties = Set(party),
        maxEvents = 100,
        startExclusive = ledgerEnd,
        endInclusive = ledgerEnd,
      )

      val v1LedgerEnd =
        com.daml.ledger.api.v1.ledger_offset.LedgerOffset.Value.Absolute(ledgerEnd.value)

      val apiRequest = GetEventsByContractKeyRequest(
        contractKey =
          Some(api.Value(api.Value.Sum.Text(txRequest.contractKey.asInstanceOf[ValueText].value))),
        templateId = Some(
          com.daml.ledger.api.v1.value
            .Identifier(packageId, moduleName.toString, dottedName.toString)
        ),
        requestingParties = txRequest.requestingParties.toSeq,
        maxEvents = txRequest.maxEvents,
        beginExclusive = Some(com.daml.ledger.api.v1.ledger_offset.LedgerOffset(v1LedgerEnd)),
        endInclusive = Some(com.daml.ledger.api.v1.ledger_offset.LedgerOffset(v1LedgerEnd)),
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

      "default optional values" in {
        val expected = txRequest.copy(
          maxEvents = 1000,
          startExclusive = domain.LedgerOffset.LedgerBegin,
          endInclusive = domain.LedgerOffset.LedgerEnd,
        )
        val request = apiRequest.clearBeginExclusive.clearEndInclusive.withMaxEvents(0)
        validator.validateEventsByContractKey(request) shouldBe Right(expected)
      }

    }

  }
}
