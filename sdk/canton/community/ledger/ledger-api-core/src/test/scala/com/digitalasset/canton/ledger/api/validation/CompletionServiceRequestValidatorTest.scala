// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionStreamRequest as GrpcCompletionStreamRequest,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary
import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.messages.command.completion.CompletionStreamRequest
import io.grpc.Status.Code.*
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

class CompletionServiceRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with MockitoSugar {
  private implicit val noLogging: ContextualizedErrorLogger = NoLogging
  private val grpcCompletionReq = GrpcCompletionStreamRequest(
    expectedLedgerId,
    expectedApplicationId,
    List(party),
    Some(LedgerOffset(LedgerOffset.Value.Absolute(absoluteOffset))),
  )
  private val completionReq = CompletionStreamRequest(
    Some(domain.LedgerId(expectedLedgerId)),
    Ref.ApplicationId.assertFromString(expectedApplicationId),
    List(party).toSet,
    Some(domain.LedgerOffset.Absolute(Ref.LedgerString.assertFromString(absoluteOffset))),
  )

  private val endReq = CompletionEndRequest(expectedLedgerId)

  private val validator = new CompletionServiceRequestValidator(
    domain.LedgerId(expectedLedgerId),
    PartyNameChecker.AllowAllParties,
  )

  "CompletionRequestValidation" when {

    "validating gRPC completion requests" should {

      "accept requests with empty ledger ID" in {
        inside(
          validator.validateGrpcCompletionStreamRequest(grpcCompletionReq.withLedgerId(""))
        ) { case Right(req) =>
          req shouldBe completionReq.copy(ledgerId = None)
        }
      }

      "return the correct error on missing application ID" in {
        requestMustFailWith(
          request = validator.validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withApplicationId("")
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: application_id",
          metadata = Map.empty,
        )
      }

      "return the correct error on unknown begin boundary" in {
        requestMustFailWith(
          request = validator.validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withOffset(
              LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.Unrecognized(7)))
            )
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown ledger boundary value '7' in field offset.boundary",
          metadata = Map.empty,
        )
      }

      "tolerate all fields filled out" in {
        inside(
          validator.validateGrpcCompletionStreamRequest(grpcCompletionReq)
        ) { case Right(req) =>
          req shouldBe completionReq
        }
      }
    }

    "validate domain completion requests" should {

      "accept requests with empty ledger ID" in {
        inside(
          validator.validateCompletionStreamRequest(completionReq.copy(ledgerId = None), ledgerEnd)
        ) { case Right(req) =>
          req shouldBe completionReq.copy(ledgerId = None)
        }

      }

      "return the correct error on missing party" in {
        requestMustFailWith(
          request = validator.validateCompletionStreamRequest(
            completionReq.copy(parties = Set.empty),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: parties",
          metadata = Map.empty,
        )
      }

      "return the correct error when offset is after ledger end" in {
        requestMustFailWith(
          request = validator.validateCompletionStreamRequest(
            completionReq.copy(offset =
              Some(
                domain.LedgerOffset.Absolute(
                  Ref.LedgerString.assertFromString((ledgerEnd.value.toInt + 1).toString)
                )
              )
            ),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            "OFFSET_AFTER_LEDGER_END(12,0): Begin offset (1001) is after ledger end (1000)",
          metadata = Map.empty,
        )
      }

      "tolerate missing end" in {
        inside(
          validator.validateCompletionStreamRequest(
            completionReq.copy(offset = None),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.ledgerId shouldEqual Some(expectedLedgerId)
          req.applicationId shouldEqual expectedApplicationId
          req.parties shouldEqual Set(party)
          req.offset shouldEqual None
        }
      }
    }

    "validating completions end requests" should {

      "fail on ledger ID mismatch" in {
        requestMustFailWith(
          request =
            validator.validateCompletionEndRequest(endReq.withLedgerId("mismatchedLedgerId")),
          code = NOT_FOUND,
          description =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID 'mismatchedLedgerId' not found. Actual Ledger ID is 'expectedLedgerId'.",
          metadata = Map.empty,
        )
      }

      "return passed ledger ID" in {
        inside(
          validator.validateCompletionEndRequest(endReq)
        ) { case Right(_) =>
          succeed
        }
      }
    }

    "applying party name checks" should {
      val partyRestrictiveValidator = new CompletionServiceRequestValidator(
        domain.LedgerId(expectedLedgerId),
        PartyNameChecker.AllowPartySet(Set(party)),
      )

      val unknownParties = List("party", "Alice", "Bob").map(Ref.Party.assertFromString).toSet
      val knownParties = List("party").map(Ref.Party.assertFromString)

      "reject completion requests for unknown parties" in {
        requestMustFailWith(
          request = partyRestrictiveValidator.validateCompletionStreamRequest(
            completionReq.copy(parties = unknownParties),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
          metadata = Map.empty,
        )
      }

      "accept transaction requests for known parties" in {
        partyRestrictiveValidator.validateGrpcCompletionStreamRequest(
          grpcCompletionReq.withParties(knownParties)
        ) shouldBe a[Right[_, _]]
        partyRestrictiveValidator.validateGrpcCompletionStreamRequest(
          grpcCompletionReq.withParties(knownParties)
        ) shouldBe a[Right[_, _]]
      }
    }
  }
}
