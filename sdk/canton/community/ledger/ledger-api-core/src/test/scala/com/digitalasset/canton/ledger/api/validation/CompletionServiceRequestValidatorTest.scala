// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamRequest as GrpcCompletionStreamRequest
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset.ParticipantBoundary
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.messages.command.completion.CompletionStreamRequest
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
    Some(ParticipantOffset(ParticipantOffset.Value.Absolute(absoluteOffset))),
  )
  private val completionReq = CompletionStreamRequest(
    Ref.ApplicationId.assertFromString(expectedApplicationId),
    List(party).toSet,
    Some(domain.ParticipantOffset.Absolute(Ref.LedgerString.assertFromString(absoluteOffset))),
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

      "return the correct error on unknown begin boundary" in {
        requestMustFailWith(
          request = validator.validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withBeginExclusive(
              ParticipantOffset(
                ParticipantOffset.Value.Boundary(ParticipantBoundary.Unrecognized(7))
              )
            )
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Unknown ledger boundary value '7' in field offset.boundary",
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
              Some(
                domain.ParticipantOffset.Absolute(
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
          req.applicationId shouldEqual expectedApplicationId
          req.parties shouldEqual Set(party)
          req.offset shouldEqual None
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
