// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, ErrorCodesVersionSwitcher, NoLogging}
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
    domain.LedgerId(expectedLedgerId),
    domain.ApplicationId(Ref.ApplicationId.assertFromString(expectedApplicationId)),
    List(party).toSet,
    Some(domain.LedgerOffset.Absolute(Ref.LedgerString.assertFromString(absoluteOffset))),
  )

  private val endReq = CompletionEndRequest(expectedLedgerId)

  private val errorCodesVersionSwitcher_mock = mock[ErrorCodesVersionSwitcher]
  private val validator = new CompletionServiceRequestValidator(
    domain.LedgerId(expectedLedgerId),
    PartyNameChecker.AllowAllParties,
    errorCodesVersionSwitcher = errorCodesVersionSwitcher_mock,
  )

  val fixture = new ValidatorFixture((selfServiceErrorCodesEnabled: Boolean) => {
    new CompletionServiceRequestValidator(
      domain.LedgerId(expectedLedgerId),
      PartyNameChecker.AllowAllParties,
      errorCodesVersionSwitcher = new ErrorCodesVersionSwitcher(selfServiceErrorCodesEnabled),
    )
  })

  "CompletionRequestValidation" when {

    "validating gRPC completion requests" should {

      "reject requests with empty ledger ID" in {
        fixture.testRequestFailure(
          testedRequest = _.validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withLedgerId("")
          ),
          expectedCodeV1 = NOT_FOUND,
          expectedDescriptionV1 = "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
          expectedCodeV2 = NOT_FOUND,
          expectedDescriptionV2 =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "return the correct error on missing application ID" in {
        fixture.testRequestFailure(
          testedRequest = _.validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withApplicationId("")
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: application_id",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
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
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 =
            "Invalid argument: Unknown ledger boundary value '7' in field offset.boundary",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown ledger boundary value '7' in field offset.boundary",
        )
      }

      "tolerate all fields filled out" in {
        inside(
          validator.validateGrpcCompletionStreamRequest(grpcCompletionReq)
        ) { case Right(req) =>
          req shouldBe completionReq
          verifyZeroInteractions(errorCodesVersionSwitcher_mock)
        }
      }
    }

    "validate domain completion requests" should {

      "reject requests with empty ledger ID" in {
        fixture.testRequestFailure(
          testedRequest = _.validateCompletionStreamRequest(
            completionReq.copy(ledgerId = domain.LedgerId("")),
            ledgerEnd,
          ),
          expectedCodeV1 = NOT_FOUND,
          expectedDescriptionV1 = "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
          expectedCodeV2 = NOT_FOUND,
          expectedDescriptionV2 =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "return the correct error on missing party" in {
        fixture.testRequestFailure(
          testedRequest = _.validateCompletionStreamRequest(
            completionReq.copy(parties = Set.empty),
            ledgerEnd,
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: parties",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
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
          expectedCodeV1 = OUT_OF_RANGE,
          expectedDescriptionV1 = "Begin offset (1001) is after ledger end (1000)",
          expectedCodeV2 = OUT_OF_RANGE,
          expectedDescriptionV2 =
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
          req.ledgerId shouldEqual expectedLedgerId
          req.applicationId shouldEqual expectedApplicationId
          req.parties shouldEqual Set(party)
          req.offset shouldEqual None
          verifyZeroInteractions(errorCodesVersionSwitcher_mock)
        }
      }
    }

    "validating completions end requests" should {

      "fail on ledger ID mismatch" in {
        fixture.testRequestFailure(
          testedRequest = _.validateCompletionEndRequest(endReq.withLedgerId("")),
          expectedCodeV1 = NOT_FOUND,
          expectedDescriptionV1 = "Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
          expectedCodeV2 = NOT_FOUND,
          expectedDescriptionV2 =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID '' not found. Actual Ledger ID is 'expectedLedgerId'.",
        )
      }

      "return passed ledger ID" in {
        inside(
          validator.validateCompletionEndRequest(endReq)
        ) { case Right(out) =>
          out should have(Symbol("ledgerId")(expectedLedgerId))
          verifyZeroInteractions(errorCodesVersionSwitcher_mock)
        }
      }
    }

    "applying party name checks" should {

      val knowsPartyOnlyFixture = new ValidatorFixture(enabled => {
        new CompletionServiceRequestValidator(
          domain.LedgerId(expectedLedgerId),
          PartyNameChecker.AllowPartySet(Set(party)),
          new ErrorCodesVersionSwitcher(enabled),
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
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Invalid argument: Unknown parties: [Alice, Bob]",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
        )
      }

      "accept transaction requests for known parties" in {
        knowsPartyOnlyFixture
          .tested(enabledSelfServiceErrorCodes = true)
          .validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withParties(knownParties)
          ) shouldBe a[Right[_, _]]
        knowsPartyOnlyFixture
          .tested(enabledSelfServiceErrorCodes = false)
          .validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withParties(knownParties)
          ) shouldBe a[Right[_, _]]
      }
    }
  }
}
