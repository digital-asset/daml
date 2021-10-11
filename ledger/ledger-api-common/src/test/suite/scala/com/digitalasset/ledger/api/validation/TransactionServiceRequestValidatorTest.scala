// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

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
import io.grpc.Status.Code._
import org.scalatest.wordspec.AnyWordSpec

class TransactionServiceRequestValidatorTest extends AnyWordSpec with ValidatorTestUtils {

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

  val sut = new TransactionServiceRequestValidator(
    domain.LedgerId(expectedLedgerId),
    PartyNameChecker.AllowAllParties,
  )

  "TransactionRequestValidation" when {

    "validating regular requests" should {

      "reject requests with empty ledger ID" in {
        requestMustFailWith(
          sut.validate(txReq.withLedgerId(""), ledgerEnd),
          NOT_FOUND,
          "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "return the correct error on missing filter" in {
        requestMustFailWith(
          sut.validate(txReq.update(_.optionalFilter := None), ledgerEnd),
          INVALID_ARGUMENT,
          "Missing field: filter",
        )
      }

      "return the correct error on empty filter" in {
        requestMustFailWith(
          sut.validate(
            txReq.update(_.filter.filtersByParty := Map.empty),
            ledgerEnd,
          ),
          INVALID_ARGUMENT,
          "Invalid argument: filtersByParty cannot be empty",
        )
      }

      "return the correct error on missing begin" in {
        requestMustFailWith(
          sut.validate(txReq.update(_.optionalBegin := None), ledgerEnd),
          INVALID_ARGUMENT,
          "Missing field: begin",
        )
      }

      "return the correct error on empty begin " in {
        requestMustFailWith(
          sut.validate(txReq.update(_.begin := LedgerOffset()), ledgerEnd),
          INVALID_ARGUMENT,
          "Missing field: begin.(boundary|value)",
        )
      }

      "return the correct error on empty end " in {
        requestMustFailWith(
          sut.validate(txReq.withEnd(LedgerOffset()), ledgerEnd),
          INVALID_ARGUMENT,
          "Missing field: end.(boundary|value)",
        )
      }

      "return the correct error on unknown begin boundary" in {
        requestMustFailWith(
          sut.validate(
            txReq.withBegin(
              LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.Unrecognized(7)))
            ),
            ledgerEnd,
          ),
          INVALID_ARGUMENT,
          "Invalid argument: Unknown ledger boundary value '7' in field begin.boundary",
        )
      }

      "return the correct error on unknown end boundary" in {
        requestMustFailWith(
          sut.validate(
            txReq.withEnd(
              LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.Unrecognized(7)))
            ),
            ledgerEnd,
          ),
          INVALID_ARGUMENT,
          "Invalid argument: Unknown ledger boundary value '7' in field end.boundary",
        )
      }

      "return the correct error when begin offset is after ledger end" in {
        requestMustFailWith(
          sut.validate(
            txReq.withBegin(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          OUT_OF_RANGE,
          "Begin offset 1001 is after ledger end 1000",
        )
      }

      "return the correct error when end offset is after ledger end" in {
        requestMustFailWith(
          sut.validate(
            txReq.withEnd(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          OUT_OF_RANGE,
          "End offset 1001 is after ledger end 1000",
        )
      }

      "tolerate missing end" in {
        inside(sut.validate(txReq.update(_.optionalEnd := None), ledgerEnd)) { case Right(req) =>
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
          sut.validate(
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
          sut.validate(
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
        inside(sut.validate(txReq, ledgerEnd)) { case Right(req) =>
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
        inside(sut.validateTree(txTreeReq, ledgerEnd)) { case Right(req) =>
          req.ledgerId shouldEqual expectedLedgerId
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          req.parties should have size 1
          req.parties.headOption.value shouldEqual party
          req.verbose shouldEqual verbose
        }
      }

      "not tolerate having filters_inclusive" in {
        requestMustFailWith(
          sut.validateTree(
            txTreeReq.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.optionalInclusive := Some(InclusiveFilters()))
            })),
            ledgerEnd,
          ),
          INVALID_ARGUMENT,
          "Invalid argument: party attempted subscription for templates []. Template filtration is not supported on GetTransactionTrees RPC. To get filtered data, use the GetTransactions RPC.",
        )
      }

      "return the correct error when begin offset is after ledger end" in {
        requestMustFailWith(
          sut.validateTree(
            txTreeReq.withBegin(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          OUT_OF_RANGE,
          "Begin offset 1001 is after ledger end 1000",
        )
      }

      "return the correct error when end offset is after ledger end" in {
        requestMustFailWith(
          sut.validateTree(
            txTreeReq.withEnd(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          OUT_OF_RANGE,
          "End offset 1001 is after ledger end 1000",
        )
      }
    }

    "validating ledger end requests" should {

      "fail on ledger ID mismatch" in {
        requestMustFailWith(
          sut.validateLedgerEnd(endReq.withLedgerId("")),
          NOT_FOUND,
          "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "return passed ledger ID" in {
        inside(sut.validateLedgerEnd(endReq)) { case Right(out) =>
          out should have(Symbol("ledgerId")(expectedLedgerId))
        }
      }
    }

    "validating transaction by id requests" should {

      "fail on ledger ID mismatch" in {
        requestMustFailWith(
          sut.validateTransactionById(txByIdReq.withLedgerId("")),
          NOT_FOUND,
          "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "fail on empty transactionId" in {
        requestMustFailWith(
          sut.validateTransactionById(txByIdReq.withTransactionId("")),
          INVALID_ARGUMENT,
          "Missing field: transaction_id",
        )
      }

      "fail on empty requesting parties" in {
        requestMustFailWith(
          sut.validateTransactionById(txByIdReq.withRequestingParties(Nil)),
          INVALID_ARGUMENT,
          "Missing field: requesting_parties",
        )
      }

      "return passed ledger ID" in {
        inside(sut.validateTransactionById(txByIdReq)) { case Right(out) =>
          out should have(Symbol("ledgerId")(expectedLedgerId))
        }
      }

    }

    "validating transaction by event id requests" should {

      "fail on ledger ID mismatch" in {
        requestMustFailWith(
          sut.validateTransactionByEventId(txByEvIdReq.withLedgerId("")),
          NOT_FOUND,
          "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "fail on empty eventId" in {
        requestMustFailWith(
          sut.validateTransactionByEventId(txByEvIdReq.withEventId("")),
          INVALID_ARGUMENT,
          "Missing field: event_id",
        )
      }

      "fail on empty requesting parties" in {
        requestMustFailWith(
          sut.validateTransactionByEventId(txByEvIdReq.withRequestingParties(Nil)),
          INVALID_ARGUMENT,
          "Missing field: requesting_parties",
        )
      }

      "return passed ledger ID" in {
        inside(
          sut.validateTransactionByEventId(txByEvIdReq)
        ) { case Right(out) =>
          out should have(Symbol("ledgerId")(expectedLedgerId))
        }
      }

    }

    "applying party name checks" should {

      val knowsPartyOnly =
        new TransactionServiceRequestValidator(
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
          knowsPartyOnly.validate(txReq.withFilter(filterWithUnknown), ledgerEnd),
          INVALID_ARGUMENT,
          "Invalid argument: Unknown parties: [Alice, Bob]",
        )
      }

      "reject transaction tree requests for unknown parties" in {
        requestMustFailWith(
          knowsPartyOnly
            .validateTree(txTreeReq.withFilter(filterWithUnknown), ledgerEnd),
          INVALID_ARGUMENT,
          "Invalid argument: Unknown parties: [Alice, Bob]",
        )
      }

      "reject transaction by id requests for unknown parties" in {
        requestMustFailWith(
          knowsPartyOnly.validateTransactionById(
            txByIdReq.withRequestingParties(partyWithUnknowns)
          ),
          INVALID_ARGUMENT,
          "Invalid argument: Unknown parties: [Alice, Bob]",
        )
      }

      "reject transaction by event id requests for unknown parties" in {
        requestMustFailWith(
          knowsPartyOnly.validateTransactionById(
            txByIdReq.withRequestingParties(partyWithUnknowns)
          ),
          INVALID_ARGUMENT,
          "Invalid argument: Unknown parties: [Alice, Bob]",
        )
      }

      "accept transaction requests for known parties" in {
        knowsPartyOnly.validate(
          txReq.withFilter(filterWithKnown),
          ledgerEnd,
        ) shouldBe a[Right[_, _]]
      }

      "accept transaction tree requests for known parties" in {
        knowsPartyOnly.validateTree(
          txTreeReq.withFilter(filterWithKnown),
          ledgerEnd,
        ) shouldBe a[Right[_, _]]
      }

      "accept transaction by id requests for known parties" in {
        knowsPartyOnly.validateTransactionById(
          txByIdReq.withRequestingParties(List("party"))
        ) shouldBe a[Right[_, _]]
      }

      "accept transaction by event id requests for known parties" in {
        knowsPartyOnly.validateTransactionById(
          txByIdReq.withRequestingParties(List("party"))
        ) shouldBe a[Right[_, _]]
      }
    }
  }
}
