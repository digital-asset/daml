// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, ErrorCodesVersionSwitcher, NoLogging}
import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest,
}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.Status.Code
import io.grpc.Status.Code._
import io.grpc.StatusRuntimeException
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

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
                  )
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

  class Fixture(testedFactory: Boolean => TransactionServiceRequestValidator) {
    def testRequestFailure(
        testedRequest: TransactionServiceRequestValidator => Either[StatusRuntimeException, _],
        expectedCodeV1: Code,
        expectedDescriptionV1: String,
        expectedCodeV2: Code,
        expectedDescriptionV2: String,
    ): Assertion = {
      requestMustFailWith(
        request = testedRequest(testedFactory(false)),
        code = expectedCodeV1,
        description = expectedDescriptionV1,
      )
      requestMustFailWith(
        request = testedRequest(testedFactory(true)),
        code = expectedCodeV2,
        description = expectedDescriptionV2,
      )
    }

    def tested(enabledSelfServiceErrorCodes: Boolean): TransactionServiceRequestValidator = {
      testedFactory(enabledSelfServiceErrorCodes)
    }
  }

  private val errorCodesVersionSwitcher_mock = mock[ErrorCodesVersionSwitcher]
  private val testedValidator = new TransactionServiceRequestValidator(
    domain.LedgerId(expectedLedgerId),
    PartyNameChecker.AllowAllParties,
    ErrorFactories(errorCodesVersionSwitcher_mock),
  )

  private val fixture = new Fixture((selfServiceErrorCodesEnabled: Boolean) => {
    new TransactionServiceRequestValidator(
      domain.LedgerId(expectedLedgerId),
      PartyNameChecker.AllowAllParties,
      ErrorFactories(new ErrorCodesVersionSwitcher(selfServiceErrorCodesEnabled)),
    )
  })

  "TransactionRequestValidation" when {

    "validating regular requests" should {

      "reject requests with empty ledger ID" in {
        fixture.testRequestFailure(
          _.validate(txReq.withLedgerId(""), ledgerEnd),
          expectedCodeV1 = NOT_FOUND,
          expectedDescriptionV1 = "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
          expectedCodeV2 = NOT_FOUND,
          expectedDescriptionV2 =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "return the correct error on missing filter" in {
        fixture.testRequestFailure(
          _.validate(txReq.update(_.optionalFilter := None), ledgerEnd),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: filter",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: filter",
        )
      }

      "return the correct error on empty filter" in {
        fixture.testRequestFailure(
          _.validate(
            txReq.update(_.filter.filtersByParty := Map.empty),
            ledgerEnd,
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Invalid argument: filtersByParty cannot be empty",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: filtersByParty cannot be empty",
        )
      }

      "return the correct error on missing begin" in {
        fixture.testRequestFailure(
          _.validate(txReq.update(_.optionalBegin := None), ledgerEnd),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: begin",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: begin",
        )
      }

      "return the correct error on empty begin " in {
        fixture.testRequestFailure(
          _.validate(txReq.update(_.begin := LedgerOffset()), ledgerEnd),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: begin.(boundary|value)",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: begin.(boundary|value)",
        )
      }

      "return the correct error on empty end " in {
        fixture.testRequestFailure(
          _.validate(txReq.withEnd(LedgerOffset()), ledgerEnd),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: end.(boundary|value)",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: end.(boundary|value)",
        )
      }

      "return the correct error on unknown begin boundary" in {
        fixture.testRequestFailure(
          _.validate(
            txReq.withBegin(
              LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.Unrecognized(7)))
            ),
            ledgerEnd,
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 =
            "Invalid argument: Unknown ledger boundary value '7' in field begin.boundary",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown ledger boundary value '7' in field begin.boundary",
        )
      }

      "return the correct error on unknown end boundary" in {
        fixture.testRequestFailure(
          _.validate(
            txReq.withEnd(
              LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.Unrecognized(7)))
            ),
            ledgerEnd,
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 =
            "Invalid argument: Unknown ledger boundary value '7' in field end.boundary",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown ledger boundary value '7' in field end.boundary",
        )
      }

      "return the correct error when begin offset is after ledger end" in {
        fixture.testRequestFailure(
          _.validate(
            txReq.withBegin(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          expectedCodeV1 = OUT_OF_RANGE,
          expectedDescriptionV1 = "Begin offset 1001 is after ledger end 1000",
          expectedCodeV2 = OUT_OF_RANGE,
          expectedDescriptionV2 =
            "REQUESTED_OFFSET_OUT_OF_RANGE(12,0): Begin offset 1001 is after ledger end 1000",
        )
      }

      "return the correct error when end offset is after ledger end" in {
        fixture.testRequestFailure(
          _.validate(
            txReq.withEnd(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          expectedCodeV1 = OUT_OF_RANGE,
          expectedDescriptionV1 = "End offset 1001 is after ledger end 1000",
          expectedCodeV2 = OUT_OF_RANGE,
          expectedDescriptionV2 =
            "REQUESTED_OFFSET_OUT_OF_RANGE(12,0): End offset 1001 is after ledger end 1000",
        )
      }

      "tolerate missing end" in {
        inside(testedValidator.validate(txReq.update(_.optionalEnd := None), ledgerEnd)) {
          case Right(req) =>
            req.ledgerId shouldEqual expectedLedgerId
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
          testedValidator.validate(
            txReq.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.inclusive := InclusiveFilters(Nil))
            })),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.ledgerId shouldEqual expectedLedgerId
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          val filtersByParty = req.filter.filtersByParty
          filtersByParty should have size 1
          inside(filtersByParty.headOption.value) { case (p, filters) =>
            p shouldEqual party
            filters shouldEqual domain.Filters(Some(domain.InclusiveFilters(Set())))
          }
          req.verbose shouldEqual verbose
        }
      }

      "tolerate missing filters_inclusive" in {
        inside(
          testedValidator.validate(
            txReq.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.optionalInclusive := None)
            })),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.ledgerId shouldEqual expectedLedgerId
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
        inside(testedValidator.validate(txReq, ledgerEnd)) { case Right(req) =>
          req.ledgerId shouldEqual expectedLedgerId
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          hasExpectedFilters(req)
          req.verbose shouldEqual verbose
        }
      }
    }

    "validating tree requests" should {

      "tolerate missing filters_inclusive" in {
        inside(testedValidator.validateTree(txTreeReq, ledgerEnd)) { case Right(req) =>
          req.ledgerId shouldEqual expectedLedgerId
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          req.parties should have size 1
          req.parties.headOption.value shouldEqual party
          req.verbose shouldEqual verbose
        }
      }

      "not tolerate having filters_inclusive" in {
        fixture.testRequestFailure(
          _.validateTree(
            txTreeReq.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.optionalInclusive := Some(InclusiveFilters()))
            })),
            ledgerEnd,
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 =
            "Invalid argument: party attempted subscription for templates []. Template filtration is not supported on GetTransactionTrees RPC. To get filtered data, use the GetTransactions RPC.",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: party attempted subscription for templates []. Template filtration is not supported on GetTransactionTrees RPC. To get filtered data, use the GetTransactions RPC.",
        )
      }

      "return the correct error when begin offset is after ledger end" in {
        fixture.testRequestFailure(
          _.validateTree(
            txTreeReq.withBegin(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          expectedCodeV1 = OUT_OF_RANGE,
          expectedDescriptionV1 = "Begin offset 1001 is after ledger end 1000",
          expectedCodeV2 = OUT_OF_RANGE,
          expectedDescriptionV2 =
            "REQUESTED_OFFSET_OUT_OF_RANGE(12,0): Begin offset 1001 is after ledger end 1000",
        )
      }

      "return the correct error when end offset is after ledger end" in {
        fixture.testRequestFailure(
          _.validateTree(
            txTreeReq.withEnd(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          expectedCodeV1 = OUT_OF_RANGE,
          expectedDescriptionV1 = "End offset 1001 is after ledger end 1000",
          expectedCodeV2 = OUT_OF_RANGE,
          expectedDescriptionV2 =
            "REQUESTED_OFFSET_OUT_OF_RANGE(12,0): End offset 1001 is after ledger end 1000",
        )
      }
    }

    "validating ledger end requests" should {

      "fail on ledger ID mismatch" in {
        fixture.testRequestFailure(
          _.validateLedgerEnd(endReq.withLedgerId("")),
          expectedCodeV1 = NOT_FOUND,
          expectedDescriptionV1 = "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
          expectedCodeV2 = NOT_FOUND,
          expectedDescriptionV2 =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "return passed ledger ID" in {
        inside(testedValidator.validateLedgerEnd(endReq)) { case Right(out) =>
          out should have(Symbol("ledgerId")(expectedLedgerId))
        }
      }
    }

    "validating transaction by id requests" should {

      "fail on ledger ID mismatch" in {
        fixture.testRequestFailure(
          _.validateTransactionById(txByIdReq.withLedgerId("")),
          expectedCodeV1 = NOT_FOUND,
          expectedDescriptionV1 = "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
          expectedCodeV2 = NOT_FOUND,
          expectedDescriptionV2 =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "fail on empty transactionId" in {
        fixture.testRequestFailure(
          _.validateTransactionById(txByIdReq.withTransactionId("")),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: transaction_id",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: transaction_id",
        )
      }

      "fail on empty requesting parties" in {
        fixture.testRequestFailure(
          _.validateTransactionById(txByIdReq.withRequestingParties(Nil)),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: requesting_parties",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
        )
      }

      "return passed ledger ID" in {
        inside(testedValidator.validateTransactionById(txByIdReq)) { case Right(out) =>
          out should have(Symbol("ledgerId")(expectedLedgerId))
        }
      }

    }

    "validating transaction by event id requests" should {

      "fail on ledger ID mismatch" in {
        fixture.testRequestFailure(
          _.validateTransactionByEventId(txByEvIdReq.withLedgerId("")),
          expectedCodeV1 = NOT_FOUND,
          expectedDescriptionV1 = "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
          expectedCodeV2 = NOT_FOUND,
          expectedDescriptionV2 =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "fail on empty eventId" in {
        fixture.testRequestFailure(
          _.validateTransactionByEventId(txByEvIdReq.withEventId("")),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: event_id",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: event_id",
        )
      }

      "fail on empty requesting parties" in {
        fixture.testRequestFailure(
          _.validateTransactionByEventId(txByEvIdReq.withRequestingParties(Nil)),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: requesting_parties",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
        )
      }

      "return passed ledger ID" in {
        inside(
          testedValidator.validateTransactionByEventId(txByEvIdReq)
        ) { case Right(out) =>
          out should have(Symbol("ledgerId")(expectedLedgerId))
        }
      }

    }

    "applying party name checks" should {

      val knowsPartyOnlyFixture = new Fixture((selfServiceErrorCodesEnabled: Boolean) => {
        new TransactionServiceRequestValidator(
          domain.LedgerId(expectedLedgerId),
          PartyNameChecker.AllowPartySet(Set(party)),
          ErrorFactories(new ErrorCodesVersionSwitcher(selfServiceErrorCodesEnabled)),
        )
      })

      val partyWithUnknowns = List("party", "Alice", "Bob")
      val filterWithUnknown =
        TransactionFilter(partyWithUnknowns.map(_ -> Filters.defaultInstance).toMap)
      val filterWithKnown =
        TransactionFilter(Map(party -> Filters.defaultInstance))

      "reject transaction requests for unknown parties" in {
        knowsPartyOnlyFixture.testRequestFailure(
          _.validate(txReq.withFilter(filterWithUnknown), ledgerEnd),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Invalid argument: Unknown parties: [Alice, Bob]",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
        )
      }

      "reject transaction tree requests for unknown parties" in {
        knowsPartyOnlyFixture.testRequestFailure(
          _.validateTree(txTreeReq.withFilter(filterWithUnknown), ledgerEnd),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Invalid argument: Unknown parties: [Alice, Bob]",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
        )
      }

      "reject transaction by id requests for unknown parties" in {
        knowsPartyOnlyFixture.testRequestFailure(
          _.validateTransactionById(
            txByIdReq.withRequestingParties(partyWithUnknowns)
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Invalid argument: Unknown parties: [Alice, Bob]",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
        )
      }

      "reject transaction by event id requests for unknown parties" in {
        knowsPartyOnlyFixture.testRequestFailure(
          _.validateTransactionById(
            txByIdReq.withRequestingParties(partyWithUnknowns)
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Invalid argument: Unknown parties: [Alice, Bob]",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
        )
      }

      "accept transaction requests for known parties" in {
        knowsPartyOnlyFixture
          .tested(true)
          .validate(
            txReq.withFilter(filterWithKnown),
            ledgerEnd,
          ) shouldBe a[Right[_, _]]
      }

      "accept transaction tree requests for known parties" in {
        knowsPartyOnlyFixture
          .tested(true)
          .validateTree(
            txTreeReq.withFilter(filterWithKnown),
            ledgerEnd,
          ) shouldBe a[Right[_, _]]
      }

      "accept transaction by id requests for known parties" in {
        knowsPartyOnlyFixture
          .tested(true)
          .validateTransactionById(
            txByIdReq.withRequestingParties(List("party"))
          ) shouldBe a[Right[_, _]]
      }

      "accept transaction by event id requests for known parties" in {
        knowsPartyOnlyFixture
          .tested(true)
          .validateTransactionById(
            txByIdReq.withRequestingParties(List("party"))
          ) shouldBe a[Right[_, _]]
      }
    }
  }
}
