// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import java.time.Duration
import java.util.regex.Pattern

import com.daml.api.util.DurationConversion
import com.daml.error.ErrorCode
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test.Dummy
import com.daml.lf.data.Ref
import io.grpc.Status

import scala.concurrent.ExecutionContext

class CommandDeduplicationPeriodValidationIT extends LedgerTestSuite {

  test(
    "ValidDeduplicationDuration",
    "Submission returns OK if deduplication time is within the accepted interval",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    // Submission using the maximum allowed deduplication time
    val request = ledger.submitRequest(party, Dummy(party).create.command)
    for {
      config <- ledger.configuration()
      maxDedupTime = config.maxDeduplicationTime.get
      _ <- ledger.submit(request.update(_.commands.deduplicationTime := maxDedupTime))
    } yield {
      // No assertions to make, since the command went through as expected
    }
  })

  test(
    "NegativeDeduplicationDuration",
    "Submission with negative deduplication durations are rejected",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(
      DurationConversion.toProto(Duration.ofSeconds(-1))
    )
    assertFailedRequest(
      ledger,
      party,
      deduplicationPeriod,
      failReason = "Requests with a deduplication period represented by a negative duration",
      expectedMessage =
        "The submitted command has a field with invalid value: Invalid field deduplication_period: Duration must be positive",
      expectedCode = Status.Code.INVALID_ARGUMENT,
      expectedError = LedgerApiErrors.RequestValidation.InvalidField,
    )
  })

  test(
    "InvalidOffset",
    "Submission with deduplication periods represented by invalid offsets are rejected",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val deduplicationPeriod = DeduplicationPeriod.DeduplicationOffset(
      "invalid_offset"
    )
    assertFailedRequest(
      ledger,
      party,
      deduplicationPeriod,
      failReason = "Submitting a command with an invalid offset",
      expectedMessage =
        "Offset in deduplication_period not specified in hexadecimal: invalid_offset: the deduplication offset has to be a hexadecimal string and not invalid_offset",
      expectedCode = Status.Code.INVALID_ARGUMENT,
      expectedError = LedgerApiErrors.RequestValidation.NonHexOffset,
    )
  })

  test(
    "DeduplicationDurationExceedsMaxDeduplicationDuration",
    "Submission returns expected error codes if deduplication time is too big",
    allocate(SingleParty),
    enabled = _.commandDeduplicationFeatures.maxDeduplicationDurationEnforced,
    disabledReason = "Maximum deduplication duration is not enforced by the ledger",
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, Dummy(party).create.command)
    val expectedCode =
      expectedInvalidDeduplicationPeriodCode(ledger)
    val expectedError =
      expectedInvalidDeduplicationPeriodError(ledger)
    for {
      config <- ledger.configuration()
      maxDedupTime = config.maxDeduplicationTime.get
      failure <- ledger
        .submit(
          request.update(
            _.commands.deduplicationTime := maxDedupTime.update(
              _.seconds := maxDedupTime.seconds + 1
            )
          )
        )
        .mustFail("submitting a command with a deduplication time that is too big")
      _ = assertGrpcErrorRegex(
        ledger,
        failure,
        expectedCode,
        expectedError,
        optPattern = Some(
          Pattern.compile(
            "The given deduplication .+ exceeds the maximum deduplication .+"
          )
        ),
      )
      metadataLongestDuration = extractErrorInfoMetadataValue(failure, "longest_duration")
      // we expect that the request is accepted and the metadata value is valid
      _ <- ledger.submit(
        request.update(
          _.commands.deduplicationDuration := DurationConversion.toProto(
            Duration.parse(metadataLongestDuration)
          )
        )
      )
    } yield {}
  })

  test(
    "OffsetOutsideRange",
    "Submission with deduplication periods represented by offsets which are outside the valid range are rejected",
    allocate(SingleParty),
    enabled =
      _.commandDeduplicationFeatures.getDeduplicationPeriodSupport.offsetSupport.isOffsetConvertToDuration,
    disabledReason = "Only ledgers that convert offsets to durations fail",
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      end <- ledger.offsetBeyondLedgerEnd()
      deduplicationPeriod = DeduplicationPeriod.DeduplicationOffset(
        Ref.HexString.assertFromString(end.getAbsolute.toLowerCase)
      )
      _ <- assertFailedRequest(
        ledger,
        party,
        deduplicationPeriod,
        failReason = "Submitting a command with an invalid offset",
        expectedMessage =
          "The submitted command had an invalid deduplication period: Cannot convert deduplication offset to duration because there is no completion at given offset .*",
        expectedCode = Status.Code.INVALID_ARGUMENT,
        expectedError = LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField,
      )
    } yield {}
  })

  test(
    "OffsetPruned",
    "Submission with deduplication periods represented by offsets which are pruned are rejected",
    allocate(SingleParty),
    enabled =
      !_.commandDeduplicationFeatures.getDeduplicationPeriodSupport.offsetSupport.isOffsetNotSupported,
    disabledReason = "The ledger does not support deduplication periods represented by offsets",
    runConcurrently = false, // Pruning is involved
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      firstCreate <- ledger.create(party, Dummy(party))
      firstExercise <- ledger.exercise(party, firstCreate.exerciseDummyChoice1)
      end <- ledger.currentEnd()
      secondCreate <- ledger.create(party, Dummy(party))
      _ <- ledger.exercise(party, secondCreate.exerciseDummyChoice1)
      _ <- ledger.create(party, Dummy(party)) // move ledger end
      _ <- ledger.prune(pruneUpTo = end)
      deduplicationPeriod = DeduplicationPeriod.DeduplicationOffset(
        Ref.HexString.assertFromString(firstExercise.offset)
      )
      _ <- assertFailedRequest(
        ledger,
        party,
        deduplicationPeriod,
        failReason = "Submitting a command with a pruned offset",
        expectedMessage = ".*",
        expectedCode = Status.Code.INVALID_ARGUMENT,
        expectedError = LedgerApiErrors.RequestValidation.ParticipantPrunedDataAccessed,
      )
    } yield {}
  })

  private def assertFailedRequest(
      ledger: ParticipantTestContext,
      party: Primitive.Party,
      deduplicationPeriod: DeduplicationPeriod,
      failReason: String,
      expectedMessage: String,
      expectedCode: Status.Code,
      expectedError: ErrorCode,
  )(implicit ec: ExecutionContext) = {
    for {
      failure <- ledger
        .submit(
          ledger
            .submitRequest(party, Dummy(party).create.command)
            .update(
              _.commands.deduplicationPeriod := deduplicationPeriod
            )
        )
        .mustFail(failReason)
    } yield {
      assertGrpcErrorRegex(
        ledger,
        failure,
        expectedCode,
        expectedError,
        Some(Pattern.compile(expectedMessage)),
      )
    }
  }

  private def expectedInvalidDeduplicationPeriodCode(ledger: ParticipantTestContext) = {
    if (ledger.features.selfServiceErrorCodes) Status.Code.FAILED_PRECONDITION
    else Status.Code.INVALID_ARGUMENT
  }

  private def expectedInvalidDeduplicationPeriodError(ledger: ParticipantTestContext) = {
    if (ledger.features.selfServiceErrorCodes)
      LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField
    else LedgerApiErrors.RequestValidation.InvalidField
  }
}
