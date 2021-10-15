// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionStreamRequest,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary
import io.grpc.Status.Code._
import org.scalatest.wordspec.AnyWordSpec

class CompletionServiceRequestValidatorTest extends AnyWordSpec with ValidatorTestUtils {
  private implicit val noLogging: ContextualizedErrorLogger = NoLogging
  private val completionReq = CompletionStreamRequest(
    expectedLedgerId,
    expectedApplicationId,
    List(party),
    Some(LedgerOffset(LedgerOffset.Value.Absolute(absoluteOffset))),
  )

  private val endReq = CompletionEndRequest(expectedLedgerId)

  val validator = new CompletionServiceRequestValidator(
    domain.LedgerId(expectedLedgerId),
    PartyNameChecker.AllowAllParties,
  )

  "CompletionRequestValidation" when {

    "validating regular requests" should {

      "reject requests with empty ledger ID" in {
        requestMustFailWith(
          validator.validateCompletionStreamRequest(
            completionReq.withLedgerId(""),
            ledgerEnd,
          ),
          NOT_FOUND,
          "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "return the correct error on missing application ID" in {
        requestMustFailWith(
          validator.validateCompletionStreamRequest(
            completionReq.withApplicationId(""),
            ledgerEnd,
          ),
          INVALID_ARGUMENT,
          "Missing field: application_id",
        )
      }

      "return the correct error on missing party" in {
        requestMustFailWith(
          validator.validateCompletionStreamRequest(
            completionReq.withParties(Seq()),
            ledgerEnd,
          ),
          INVALID_ARGUMENT,
          "Missing field: parties",
        )
      }

      "return the correct error on unknown begin boundary" in {
        requestMustFailWith(
          validator.validateCompletionStreamRequest(
            completionReq.withOffset(
              LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.Unrecognized(7)))
            ),
            ledgerEnd,
          ),
          INVALID_ARGUMENT,
          "Invalid argument: Unknown ledger boundary value '7' in field offset.boundary",
        )
      }

      "return the correct error when offset is after ledger end" in {
        requestMustFailWith(
          validator.validateCompletionStreamRequest(
            completionReq.withOffset(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          OUT_OF_RANGE,
          "Begin offset 1001 is after ledger end 1000",
        )
      }

      "tolerate missing end" in {
        inside(
          validator.validateCompletionStreamRequest(
            completionReq.update(_.optionalOffset := None),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.ledgerId shouldEqual expectedLedgerId
          req.applicationId shouldEqual expectedApplicationId
          req.parties shouldEqual Set(party)
          req.offset shouldEqual None
        }
      }

      "tolerate all fields filled out" in {
        inside(
          validator.validateCompletionStreamRequest(completionReq, ledgerEnd)
        ) { case Right(req) =>
          req.ledgerId shouldEqual expectedLedgerId
          req.applicationId shouldEqual expectedApplicationId
          req.parties shouldEqual Set(party)
          req.offset shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
        }
      }
    }

    "validating completions end requests" should {

      "fail on ledger ID mismatch" in {
        requestMustFailWith(
          validator.validateCompletionEndRequest(endReq.withLedgerId("")),
          NOT_FOUND,
          "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "return passed ledger ID" in {
        inside(
          validator.validateCompletionEndRequest(endReq)
        ) { case Right(out) =>
          out should have(Symbol("ledgerId")(expectedLedgerId))
        }
      }
    }

    "applying party name checks" should {

      val knowsPartyOnly =
        new CompletionServiceRequestValidator(
          domain.LedgerId(expectedLedgerId),
          PartyNameChecker.AllowPartySet(Set(party)),
        )

      val unknownParties = List("party", "Alice", "Bob")
      val knownParties = List("party")

      "reject completion requests for unknown parties" in {
        requestMustFailWith(
          knowsPartyOnly.validateCompletionStreamRequest(
            completionReq.withParties(unknownParties),
            ledgerEnd,
          ),
          INVALID_ARGUMENT,
          "Invalid argument: Unknown parties: [Alice, Bob]",
        )
      }

      "accept transaction requests for known parties" in {
        knowsPartyOnly.validateCompletionStreamRequest(
          completionReq.withParties(knownParties),
          ledgerEnd,
        ) shouldBe a[Right[_, _]]
      }
    }
  }
}
