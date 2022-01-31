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
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test.Dummy
import com.daml.lf.data.Ref
import io.grpc.Status

import scala.concurrent.ExecutionContext

class CommandDeduplicationPeriodValidationIT extends LedgerTestSuite {

  test(
    "NegativeDeduplicationDuration",
    "Submission with negative deduplication durations are rejected",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(
      DurationConversion.toProto(Duration.ofSeconds(-1))
    )
    assertSyncFailedRequest(
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
    assertSyncFailedRequest(
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
      _ <- assertSyncFailedRequest(
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
    def submitAndWaitWithDeduplication(
        deduplicationPeriod: DeduplicationPeriod.DeduplicationOffset
    ) = {
      ledger
        .submitAndWait(
          ledger
            .submitAndWaitRequest(party, Dummy(party).create.command)
            .update(
              _.commands.deduplicationPeriod := deduplicationPeriod
            )
        )
    }
    val isOffsetNativelySupported =
      ledger.features.commandDeduplicationFeatures.getDeduplicationPeriodSupport.offsetSupport.isOffsetNativeSupport
    for {
      start <- ledger.currentEnd()
      firstCreate <- ledger.create(party, Dummy(party))
      _ <- ledger.exercise(party, firstCreate.exerciseDummyChoice1)
      secondCreate <- ledger.create(party, Dummy(party))
      _ <- ledger.submitAndWait(ledger.submitAndWaitRequest(party, Dummy(party).create.command))
      end <- ledger.currentEnd()
      _ <- ledger.exercise(party, secondCreate.exerciseDummyChoice1)
      _ <- ledger.create(party, Dummy(party)) // move ledger end
      _ <- ledger.submitAndWait(ledger.submitAndWaitRequest(party, Dummy(party).create.command))
      _ <- ledger.prune(pruneUpTo = end)
      failure <- submitAndWaitWithDeduplication(
        DeduplicationPeriod.DeduplicationOffset(
          start.getAbsolute
        )
      ).mustFail("using an offset which was pruned")
      _ = assertGrpcErrorRegex(
        ledger,
        failure,
        expectedCode = Status.Code.FAILED_PRECONDITION,
        // Canton returns INVALID_DEDUPLICATION_PERIOD with earliest_offset metadata
        // KV returns PARTICIPANT_PRUNED_DATA_ACCESSED with earliest_offset metadata
        selfServiceErrorCode =
          if (isOffsetNativelySupported)
            LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField
          else LedgerApiErrors.RequestValidation.ParticipantPrunedDataAccessed,
        None,
      )
      earliestOffset = extractErrorInfoMetadataValue(
        failure,
        LedgerApiErrors.EarliestOffsetMetadataKey,
      )
      _ <-
        // Because KV treats deduplication offsets as inclusive, and because the participant pruning offset is inclusive
        // we cannot simply use the received offset as a deduplication period, but we have to find the first completion after the given offset
        if (isOffsetNativelySupported) {
          submitAndWaitWithDeduplication(
            DeduplicationPeriod.DeduplicationOffset(earliestOffset)
          )
        } else {
          findFirstOffsetAfterGivenOffset(ledger, earliestOffset)(party).flatMap(offset =>
            submitAndWaitWithDeduplication(DeduplicationPeriod.DeduplicationOffset(offset))
          )
        }
    } yield {}
  })

  private def findFirstOffsetAfterGivenOffset(ledger: ParticipantTestContext, offset: String)(
      party: Primitive.Party
  )(implicit ec: ExecutionContext) = {
    ledger
      .findCompletion(
        ledger.completionStreamRequest(
          LedgerOffset.of(LedgerOffset.Value.Absolute(offset))
        )(party)
      )(_ => true)
      .map { completionOpt =>
        {
          val completion = assertDefined(completionOpt, "No completion found")
          completion.offset.getAbsolute
        }
      }
  }

  private def assertSyncFailedRequest(
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
}
