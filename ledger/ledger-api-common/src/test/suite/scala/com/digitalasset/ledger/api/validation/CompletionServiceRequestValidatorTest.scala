// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.domain
import com.daml.ledger.api.messages.command.completion.CompletionStreamRequest
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionStreamRequest => GrpcCompletionStreamRequest,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary
import com.daml.lf.data.Ref
import io.grpc.Status.Code._
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

  val fixture = new ValidatorFixture(() => {
    new CompletionServiceRequestValidator(
      domain.LedgerId(expectedLedgerId),
      PartyNameChecker.AllowAllParties,
    )
  })

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
        fixture.testRequestFailure(
          testedRequest = _.validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withApplicationId("")
          ),
          expectedCode = INVALID_ARGUMENT,
          expectedDescription =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: application_id",
        )
      }

      "return the correct error on unknown begin boundary" in {
        fixture.testRequestFailure(
          testedRequest = _.validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withOffset(
              LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.Unrecognized(7)))
            )
          ),
          expectedCode = INVALID_ARGUMENT,
          expectedDescription =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown ledger boundary value '7' in field offset.boundary",
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
        fixture.testRequestFailure(
          testedRequest = _.validateCompletionStreamRequest(
            completionReq.copy(parties = Set.empty),
            ledgerEnd,
          ),
          expectedCode = INVALID_ARGUMENT,
          expectedDescription =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: parties",
        )
      }

      "return the correct error when offset is after ledger end" in {
        fixture.testRequestFailure(
          testedRequest = _.validateCompletionStreamRequest(
            completionReq.copy(offset =
              Some(
                domain.LedgerOffset.Absolute(
                  Ref.LedgerString.assertFromString((ledgerEnd.value.toInt + 1).toString)
                )
              )
            ),
            ledgerEnd,
          ),
          expectedCode = OUT_OF_RANGE,
          expectedDescription =
            "OFFSET_AFTER_LEDGER_END(12,0): Begin offset (1001) is after ledger end (1000)",
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
        fixture.testRequestFailure(
          testedRequest = _.validateCompletionEndRequest(endReq.withLedgerId("mismatchedLedgerId")),
          expectedCode = NOT_FOUND,
          expectedDescription =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID 'mismatchedLedgerId' not found. Actual Ledger ID is 'expectedLedgerId'.",
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

      val knowsPartyOnlyFixture = new ValidatorFixture(() => {
        new CompletionServiceRequestValidator(
          domain.LedgerId(expectedLedgerId),
          PartyNameChecker.AllowPartySet(Set(party)),
        )
      })

      val unknownParties = List("party", "Alice", "Bob").map(Ref.Party.assertFromString).toSet
      val knownParties = List("party").map(Ref.Party.assertFromString)

      "reject completion requests for unknown parties" in {
        knowsPartyOnlyFixture.testRequestFailure(
          testedRequest = _.validateCompletionStreamRequest(
            completionReq.copy(parties = unknownParties),
            ledgerEnd,
          ),
          expectedCode = INVALID_ARGUMENT,
          expectedDescription =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
        )
      }

      "accept transaction requests for known parties" in {
        knowsPartyOnlyFixture
          .tested()
          .validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withParties(knownParties)
          ) shouldBe a[Right[_, _]]
        knowsPartyOnlyFixture
          .tested()
          .validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withParties(knownParties)
          ) shouldBe a[Right[_, _]]
      }
    }
  }
}
