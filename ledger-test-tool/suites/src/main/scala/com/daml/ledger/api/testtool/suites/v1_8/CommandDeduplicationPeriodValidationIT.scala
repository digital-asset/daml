// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.time.Duration
import java.util.regex.Pattern
import com.daml.api.util.DurationConversion
import com.daml.error.ErrorCode
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{FutureAssertions, LedgerTestSuite}
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.javaapi.data.Party
import com.daml.ledger.test.java.model.test.Dummy
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class CommandDeduplicationPeriodValidationIT extends LedgerTestSuite {
  import CompanionImplicits._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  test(
    "ValidDeduplicationDuration",
    "Submission returns OK if deduplication time is within the accepted interval",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    // Submission using the maximum allowed deduplication time
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    for {
      config <- ledger.configuration()
      maxDedupDuration = config.maxDeduplicationDuration.get
      _ <- ledger.submit(request.update(_.commands.deduplicationTime := maxDedupDuration))
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
    assertSyncFailedRequest(
      ledger,
      party,
      deduplicationPeriod,
      failReason = "Requests with a deduplication period represented by a negative duration",
      expectedMessage =
        "The submitted command has a field with invalid value: Invalid field deduplication_period: Duration must be positive",
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
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    val expectedError = LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField
    for {
      config <- ledger.configuration()
      maxDedupDuration = config.maxDeduplicationDuration.get
      failure <- ledger
        .submit(
          request.update(
            _.commands.deduplicationTime := maxDedupDuration.update(
              _.seconds := maxDedupDuration.seconds + 1
            )
          )
        )
        .mustFail("submitting a command with a deduplication time that is too big")
      _ = assertGrpcErrorRegex(
        failure,
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
      _ <- assertSyncFailedRequest(
        ledger,
        party,
        deduplicationPeriod,
        failReason = "Submitting a command with an invalid offset",
        expectedMessage =
          "The submitted command had an invalid deduplication period: Cannot convert deduplication offset to duration because there is no completion at given offset .*",
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
            .submitAndWaitRequest(party, new Dummy(party).create.commands)
            .update(
              _.commands.deduplicationPeriod := deduplicationPeriod
            )
        )
    }
    val isOffsetNativelySupported =
      ledger.features.commandDeduplicationFeatures.getDeduplicationPeriodSupport.offsetSupport.isOffsetNativeSupport
    for {
      start <- ledger.currentEnd()
      firstCreate <- ledger.create(party, new Dummy(party))
      _ <- ledger.exercise(party, firstCreate.exerciseDummyChoice1())
      secondCreate: Dummy.ContractId <- ledger.create(party, new Dummy(party))
      _ <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(party, new Dummy(party).create.commands)
      )
      end <- ledger.currentEnd()
      _ <- ledger.exercise(party, secondCreate.exerciseDummyChoice1())
      _ <- FutureAssertions.succeedsEventually(
        retryDelay = 10.millis,
        maxRetryDuration = 10.seconds,
        ledger.delayMechanism,
        "Prune offsets",
      ) {
        for {
          _ <- ledger.create(party, new Dummy(party))
          _ <- ledger.submitAndWait(
            ledger.submitAndWaitRequest(party, new Dummy(party).create.commands)
          )
          _ <- ledger.prune(pruneUpTo = end, attempts = 1)
        } yield {}
      }
      failure <- submitAndWaitWithDeduplication(
        DeduplicationPeriod.DeduplicationOffset(
          start.getAbsolute
        )
      ).mustFail("using an offset which was pruned")
      _ = assertGrpcErrorRegex(
        failure,
        // Canton returns INVALID_DEDUPLICATION_PERIOD with earliest_offset metadata
        // KV returns PARTICIPANT_PRUNED_DATA_ACCESSED with earliest_offset metadata
        errorCode =
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
      party: Party
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
      party: Party,
      deduplicationPeriod: DeduplicationPeriod,
      failReason: String,
      expectedMessage: String,
      expectedError: ErrorCode,
  )(implicit ec: ExecutionContext) = {
    for {
      failure <- ledger
        .submit(
          ledger
            .submitRequest(party, new Dummy(party).create.commands)
            .update(
              _.commands.deduplicationPeriod := deduplicationPeriod
            )
        )
        .mustFail(failReason)
    } yield {
      assertGrpcErrorRegex(
        failure,
        expectedError,
        Some(Pattern.compile(expectedMessage)),
      )
    }
  }
}
