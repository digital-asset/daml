// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters.*
import com.daml.ledger.api.testtool.infrastructure.assertions.CommandDeduplicationAssertions.DurationConversion
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{
  FutureAssertions,
  LedgerTestSuite,
  Party,
  TestConstraints,
}
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod
import com.daml.ledger.test.java.model.test.Dummy
import com.daml.logging.LoggingContext
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors

import java.time.Duration
import java.util.regex.Pattern
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

class CommandDeduplicationPeriodValidationIT extends LedgerTestSuite {
  import CompanionImplicits.*

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  test(
    "ValidDeduplicationDuration",
    "Submission returns OK if deduplication time is positive",
    allocate(SingleParty),
  )(_ => { case Participants(Participant(ledger, Seq(party))) =>
    // Submission using the maximum allowed deduplication time
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    val maxDedupDuration = Duration.ofMinutes(30).asProtobuf
    ledger.submit(request.update(_.commands.deduplicationDuration := maxDedupDuration))
  })

  test(
    "NegativeDeduplicationDuration",
    "Submission with negative deduplication durations are rejected",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
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
      expectedError = RequestValidationErrors.InvalidField,
    )
  })

  test(
    "NegativeOffset",
    "Submission with deduplication periods represented by negative offsets are rejected",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val offset = -12345678L
    val deduplicationPeriod = DeduplicationPeriod.DeduplicationOffset(offset)
    assertSyncFailedRequest(
      ledger,
      party,
      deduplicationPeriod,
      failReason = "Submitting a command with a negative offset",
      expectedMessage =
        s"Offset $offset in deduplication_period is a negative integer: the deduplication offset has to be a non-negative integer and not $offset",
      expectedError = RequestValidationErrors.NegativeOffset,
    )
  })

  test(
    "ZeroOffset",
    "Submission with deduplication periods represented by zero offsets are rejected",
    allocate(SingleParty),
  )(_ => { case Participants(Participant(ledger, Seq(party))) =>
    val offset = 0L
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    val deduplicationPeriod = DeduplicationPeriod.DeduplicationOffset(offset)
    ledger.submit(request.update(_.commands.deduplicationPeriod := deduplicationPeriod))
  })

  test(
    "OffsetPruned",
    "Submission with deduplication periods represented by offsets which are pruned are rejected",
    allocate(SingleParty),
    runConcurrently = false, // Pruning is involved
    limitation = TestConstraints.GrpcOnly("Pruning not available in JSON API"),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    def submitAndWaitWithDeduplication(
        deduplicationPeriod: DeduplicationPeriod.DeduplicationOffset
    ) =
      ledger
        .submitAndWait(
          ledger
            .submitAndWaitRequest(party, new Dummy(party).create.commands)
            .update(
              _.commands.deduplicationPeriod := deduplicationPeriod
            )
        )
    for {
      start <- ledger.currentEnd()
      beforeBegin = 0L
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
        DeduplicationPeriod.DeduplicationOffset(start)
      ).mustFail("using an offset which was pruned")
      _ = assertGrpcErrorRegex(
        failure,
        // Canton returns INVALID_DEDUPLICATION_PERIOD with earliest_offset metadata
        errorCode = RequestValidationErrors.InvalidDeduplicationPeriodField,
        None,
      )
      earliestOffset = extractErrorInfoMetadataValue(
        failure,
        LedgerApiErrors.EarliestOffsetMetadataKey,
      )
      failureZero <- submitAndWaitWithDeduplication(
        DeduplicationPeriod.DeduplicationOffset(beforeBegin)
      ).mustFail("using an offset which was pruned")
      _ = assertGrpcErrorRegex(
        failureZero,
        // Canton returns INVALID_DEDUPLICATION_PERIOD with earliest_offset metadata
        errorCode = RequestValidationErrors.InvalidDeduplicationPeriodField,
        None,
      )
      earliestOffsetZero = extractErrorInfoMetadataValue(
        failureZero,
        LedgerApiErrors.EarliestOffsetMetadataKey,
      )
      _ = assert(
        earliestOffsetZero == earliestOffset,
        s"Earliest offset returned when deduplication offset is zero ($earliestOffsetZero) should be equal to $earliestOffset",
      )
      _ <-
        submitAndWaitWithDeduplication(
          DeduplicationPeriod.DeduplicationOffset(earliestOffsetZero.toLong)
        )
    } yield {}
  })

  private def assertSyncFailedRequest(
      ledger: ParticipantTestContext,
      party: Party,
      deduplicationPeriod: DeduplicationPeriod,
      failReason: String,
      expectedMessage: String,
      expectedError: ErrorCode,
  )(implicit ec: ExecutionContext) =
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
