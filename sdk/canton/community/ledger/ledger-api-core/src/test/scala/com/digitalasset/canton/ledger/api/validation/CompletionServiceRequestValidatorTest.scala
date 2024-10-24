// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamRequest as GrpcCompletionStreamRequest
import com.digitalasset.canton.ledger.api.domain.ParticipantOffset
import com.digitalasset.canton.ledger.api.messages.command.completion.CompletionStreamRequest
import com.digitalasset.canton.platform.ApiOffset
import com.digitalasset.daml.lf.data.Ref
import io.grpc.Status.Code.*
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

class CompletionServiceRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with MockitoSugar {
  private implicit val noLogging: ContextualizedErrorLogger = NoLogging
  private val grpcCompletionReq = GrpcCompletionStreamRequest(
    expectedApplicationId,
    List(party),
    offsetLong,
  )
  private val completionReq = CompletionStreamRequest(
    Ref.ApplicationId.assertFromString(expectedApplicationId),
    List(party).toSet,
    offset,
  )

  private val validator = new CompletionServiceRequestValidator(
    PartyNameChecker.AllowAllParties
  )

  "CompletionRequestValidation" when {

    "validating gRPC completion requests" should {

      "accept plain requests" in {
        inside(
          validator.validateGrpcCompletionStreamRequest(grpcCompletionReq)
        ) { case Right(req) =>
          req shouldBe completionReq
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

      "accept requests with begin exclusive offset zero" in {
        inside(
          validator.validateGrpcCompletionStreamRequest(grpcCompletionReq.withBeginExclusive(0))
        ) { case Right(req) =>
          req shouldBe completionReq.copy(offset = ParticipantOffset.fromString(""))
        }
      }

      "return the correct error on negative begin exclusive offset" in {
        requestMustFailWith(
          request = validator.validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withBeginExclusive(-100)
          ),
          code = INVALID_ARGUMENT,
          description =
            "NEGATIVE_OFFSET(8,0): Offset -100 in begin_exclusive is a negative integer: the offset in begin_exclusive field has to be a non-negative integer (>=0)",
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

      "tolerate empty offset (participant begin)" in {
        inside(
          validator.validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withBeginExclusive(0L)
          )
        ) { case Right(req) =>
          req.applicationId shouldEqual expectedApplicationId
          req.parties shouldEqual Set(party)
          req.offset shouldEqual ""
        }
      }

    }

    "validate domain completion requests" should {

      "accept simple requests" in {
        inside(
          validator.validateCompletionStreamRequest(completionReq, ledgerEnd)
        ) { case Right(req) =>
          req shouldBe completionReq
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
              ParticipantOffset.fromString(
                ApiOffset.fromLong(ApiOffset.assertFromStringToLong(ledgerEnd) + 1)
              )
            ),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            "OFFSET_AFTER_LEDGER_END(12,0): Begin offset (000000000000001001) is after ledger end (000000000000001000)",
          metadata = Map.empty,
        )
      }

      "tolerate empty offset (participant begin)" in {
        inside(
          validator.validateCompletionStreamRequest(
            completionReq.copy(offset = ParticipantOffset.ParticipantBegin),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.applicationId shouldEqual expectedApplicationId
          req.parties shouldEqual Set(party)
          req.offset shouldEqual ""
        }
      }
    }

    "applying party name checks" should {
      val partyRestrictiveValidator = new CompletionServiceRequestValidator(
        PartyNameChecker.AllowPartySet(Set(party))
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
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Unknown parties: [Alice, Bob]",
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
