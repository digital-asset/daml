// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.{Participant, *}
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{FutureAssertions, LedgerTestSuite, Party}
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdRequest
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.model
import com.daml.ledger.test.java.semantic.divulgencetests
import com.daml.ledger.test.java.semantic.divulgencetests.{Contract, Dummy}
import com.daml.logging.LoggingContext
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors

import java.util.regex.Pattern
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class ParticipantPruningIT extends LedgerTestSuite {
  import CompanionImplicits.*
  implicit val contractCompanion
      : ContractCompanion.WithoutKey[Contract.Contract$, Contract.ContractId, Contract] =
    Contract.COMPANION
  implicit val semanticTestsDummyCompanion: ContractCompanion.WithoutKey[
    divulgencetests.Dummy.Contract,
    divulgencetests.Dummy.ContractId,
    divulgencetests.Dummy,
  ] = divulgencetests.Dummy.COMPANION

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  // One point of populating the ledger with a lot of events is to help advance canton's safe-pruning offsets
  private val batchesToPopulate = 74

  private val lastItemToPruneIndex = batchesToPopulate

  test(
    "PRFailPruneByNoOffset",
    "Pruning a participant without specifying an offset should fail",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(participant, Seq())) =>
    for {
      failure <- participant
        .prune(
          0,
          attempts = 1,
        )
        .mustFail("pruning without specifying an offset")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.InvalidArgument,
        Some("prune_up_to not specified or zero"),
      )
    }
  })

  test(
    "PRQueryLatestPrunedOffsets",
    "It should be possible to query the latest pruned offsets",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    for {
      (prunedUpToInclusive_initial, allDivulgencePrunedUpToInclusive_initial) <-
        ledger.latestPrunedOffsets()

      // Move the ledger end forward
      _ <- ledger.create(alice, new Dummy(alice))
      firstPruningOffset <- ledger.currentEnd()

      // Add one more element to bypass pruning to ledger end restriction
      // and allow pruning at the first pruning offset
      _ <- ledger.create(alice, new Dummy(alice))
      secondPruningOffset <- ledger.currentEnd()

      // Add one element to bypass pruning to ledger end restriction
      // and allow pruning at the second pruning offset
      _ <- ledger.create(alice, new Dummy(alice))

      // Prune the ledger at firstPruningOffset
      _ <- ledger.pruneCantonSafe(
        firstPruningOffset,
        alice,
        p => new Dummy(p).create.commands,
      )
      (
        prunedUpToInclusive_afterFirstPruning,
        allDivulgencePrunedUpToInclusive_afterFirstPruning,
      ) <- ledger.latestPrunedOffsets()

      // Prune the ledger with divulgence at secondPruningOffset
      _ <- ledger.pruneCantonSafe(
        secondPruningOffset,
        alice,
        p => new Dummy(p).create.commands,
      )
      (
        prunedUpToInclusive_afterSecondPruning,
        allDivulgencePrunedUpToInclusive_afterSecondPruning,
      ) <- ledger.latestPrunedOffsets()
    } yield {
      assert(
        assertion = prunedUpToInclusive_initial < prunedUpToInclusive_afterFirstPruning,
        message =
          s"The initial pruning offset ($prunedUpToInclusive_initial) should be different than the latest pruning offset ($prunedUpToInclusive_afterFirstPruning)",
      )

      assertEquals(
        "Requested pruning offset matches the queried offset",
        prunedUpToInclusive_afterFirstPruning,
        firstPruningOffset,
      )

      assertEquals(
        "All divulgence pruning offset matches the requested pruning offset with all divulgence pruning enabled",
        allDivulgencePrunedUpToInclusive_afterFirstPruning,
        firstPruningOffset,
      )

      assert(
        assertion =
          allDivulgencePrunedUpToInclusive_afterFirstPruning < allDivulgencePrunedUpToInclusive_afterSecondPruning,
        message =
          "Divulgence pruning offset advanced as well after the second prune call with all divulgence pruning enabled",
      )

      assertEquals(
        "Pruning offsets are equal after all divulgence pruning",
        allDivulgencePrunedUpToInclusive_afterSecondPruning,
        prunedUpToInclusive_afterSecondPruning,
      )
    }
  })

  test(
    "PRFailPruneByNegativeOffset",
    "Pruning a participant specifying a negative offset should fail",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(participant, Seq())) =>
    val negativeOffset: Long = -134134134L
    for {
      cannotPruneNegativeOffset <- participant
        .prune(
          negativeOffset,
          attempts = 1,
        )
        .mustFail("pruning, specifying a negative offset")
    } yield {
      assertGrpcError(
        cannotPruneNegativeOffset,
        RequestValidationErrors.NonPositiveOffset,
        Some(s"Offset $negativeOffset in prune_up_to is not a positive integer"),
      )
    }
  })

  test(
    "PRFailPruneByOutOfBoundsOffset",
    "Pruning a participant specifying an offset after the ledger end should fail",
    allocate(NoParties),
    runConcurrently =
      false, // in spite of being a negative test, cannot be run concurrently as otherwise ledger end grows
  )(implicit ec => { case Participants(Participant(participant, Seq())) =>
    eventually("participantEndShouldNotIncrease") {
      for {
        endBefore <- participant.currentEnd()
        cannotPruneOffsetBeyondEnd <- participant
          .prune(endBefore, attempts = 1)
          .mustFail("pruning, specifying an offset after the ledger end")
        endAfter <- participant.currentEnd()
      } yield {
        // participant end should not have been increased after we have retrieved it since it can lead to a different error
        assertEquals(endAfter, endBefore)
        assertGrpcError(
          cannotPruneOffsetBeyondEnd,
          RequestValidationErrors.OffsetOutOfRange,
          Some("prune_up_to needs to be before ledger end"),
        )
      }
    }
  })

  test(
    "PRPruneTxLedgerEffects",
    "Prune succeeds as observed by transaction (LedgerEffects) lookups",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, Seq(submitter))) =>
    for {
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)
      offsetOfSecondToLastPrunedTransaction = offsets(
        lastItemToPruneIndex - 1
      ) // This offset is the largest exclusive offset we can no longer read from after
      offsetOfFirstSurvivingTransaction = offsets(lastItemToPruneIndex + 1)

      _ <- participant.prune(offsetToPruneUpTo)

      txReqAfterPrune <- participant
        .getTransactionsRequest(
          transactionFormat = participant
            .transactionFormat(
              parties = Some(Seq(submitter)),
              transactionShape = LedgerEffects,
            ),
          begin = offsetToPruneUpTo,
        )
      transactionsAfterPrune <- participant.transactions(txReqAfterPrune)

      txReq <- participant
        .getTransactionsRequest(
          transactionFormat = participant.transactionFormat(
            parties = Some(Seq(submitter)),
            transactionShape = LedgerEffects,
          ),
          begin = offsetOfSecondToLastPrunedTransaction,
        )
      cannotReadAnymore <- participant
        .transactions(txReq)
        .mustFail("attempting to read transactions before the pruning cut-off")

      cannotReadFromParticipantBegin <- participant
        .transactions(txReq.update(_.beginExclusive := 0L))
        .mustFail(
          "attempting to read transactions from participant begin after pruning has occurred"
        )
    } yield {
      assert(
        transactionsAfterPrune.head.offset == offsetOfFirstSurvivingTransaction,
        s"transactions not pruned at expected offset",
      )
      assertGrpcErrorRegex(
        cannotReadAnymore,
        RequestValidationErrors.ParticipantPrunedDataAccessed,
        Some(
          Pattern.compile(
            s"(Transactions request from [0-9]* to [0-9]* precedes pruned offset $offsetToPruneUpTo)|(Request from [0-9]* precedes pruned offset $offsetToPruneUpTo)"
          )
        ),
      )
      assertGrpcErrorRegex(
        cannotReadFromParticipantBegin,
        RequestValidationErrors.ParticipantPrunedDataAccessed,
        Some(
          Pattern.compile(
            s"(Transactions request from [0-9]* to [0-9]* precedes pruned offset $offsetToPruneUpTo)|(Request from [0-9]* precedes pruned offset $offsetToPruneUpTo)"
          )
        ),
      )
    }
  })

  test(
    "PRPruneTxAcsDelta",
    "Prune succeeds as observed by transaction (AcsDelta) lookups",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, Seq(submitter))) =>
    for {
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)
      offsetOfSecondToLastPrunedTransaction = offsets(
        lastItemToPruneIndex - 1
      ) // This offset is the largest exclusive offset we can no longer read from after
      offsetOfFirstSurvivingTransaction = offsets(lastItemToPruneIndex + 1)

      _ <- participant.prune(offsetToPruneUpTo)

      txReqAfterPrune <- participant
        .getTransactionsRequest(
          transactionFormat = participant.transactionFormat(parties = Some(Seq(submitter))),
          begin = offsetToPruneUpTo,
        )
      txAfterPrune <- participant.transactions(txReqAfterPrune)

      txReq <- participant
        .getTransactionsRequest(
          transactionFormat = participant.transactionFormat(parties = Some(Seq(submitter))),
          begin = offsetOfSecondToLastPrunedTransaction,
        )
      cannotReadAnymore <- participant
        .transactions(txReq)
        .mustFail("attempting to read transactions before the pruning cut-off")

      cannotReadFromParticipantBegin <- participant
        .transactions(txReq.update(_.beginExclusive := 0L))
        .mustFail(
          "attempting to read transactions from participant begin after pruning has occurred"
        )
    } yield {
      assert(
        txAfterPrune.head.offset == offsetOfFirstSurvivingTransaction,
        s"flat transactions not pruned at expected offset",
      )
      assertGrpcErrorRegex(
        cannotReadAnymore,
        RequestValidationErrors.ParticipantPrunedDataAccessed,
        Some(
          Pattern.compile(
            s"(Transactions request from [0-9]* to [0-9]* precedes pruned offset $offsetToPruneUpTo)|(Request from [0-9]* precedes pruned offset $offsetToPruneUpTo)"
          )
        ),
      )
      assertGrpcErrorRegex(
        cannotReadFromParticipantBegin,
        RequestValidationErrors.ParticipantPrunedDataAccessed,
        Some(
          Pattern.compile(
            s"(Transactions request from [0-9]* to [0-9]* precedes pruned offset $offsetToPruneUpTo)|(Request from [0-9]* precedes pruned offset $offsetToPruneUpTo)"
          )
        ),
      )
    }
  })

  test(
    "PRPruneCompletions",
    "Prune succeeds as observed by command completions",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, Seq(submitter))) =>
    for {
      endOffsetAtTestStart <- participant.currentEnd()
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)
      offsetOfSecondToLastPrunedCheckpoint = offsets(
        lastItemToPruneIndex - 1
      ) // This offset is the largest exclusive offset we can no longer read from after
      offsetOfFirstSurvivingCheckpoint = offsets(lastItemToPruneIndex + 1)

      firstCheckpointBeforePrune <- participant
        .offsets(
          1,
          participant.completionStreamRequest(endOffsetAtTestStart)(submitter),
        )
        .map(_.head)

      _ <- participant.prune(offsetToPruneUpTo)

      firstCheckpointsAfterPrune <- participant
        .offsets(
          1,
          participant
            .completionStreamRequest(offsetToPruneUpTo)(submitter),
        )
        .map(_.head)

      cannotReadAnymore <- participant
        .offsets(
          1,
          participant.completionStreamRequest(offsetOfSecondToLastPrunedCheckpoint)(submitter),
        )
        .mustFail("attempting to read transactions before the pruning cut-off")

      cannotReadFromParticipantBegin <- participant
        .offsets(
          1,
          participant.completionStreamRequest(0L)(submitter),
        )
        .mustFail(
          "attempting to read transactions from participant begin after pruning has occurred"
        )
    } yield {
      assert(
        firstCheckpointBeforePrune < offsetToPruneUpTo
      )
      assert(
        firstCheckpointsAfterPrune == offsetOfFirstSurvivingCheckpoint,
        s"first checkpoint offset $firstCheckpointsAfterPrune after pruning does not match expected offset $offsetOfFirstSurvivingCheckpoint",
      )
      assertGrpcErrorRegex(
        cannotReadAnymore,
        RequestValidationErrors.ParticipantPrunedDataAccessed,
        Some(
          Pattern.compile(
            s"Command completions? request from [0-9]* to [0-9]* overlaps with pruned offset $offsetToPruneUpTo"
          )
        ),
      )
      assertGrpcErrorRegex(
        cannotReadFromParticipantBegin,
        RequestValidationErrors.ParticipantPrunedDataAccessed,
        Some(
          Pattern.compile(
            s"Command completions? request from [0-9]* to [0-9]* overlaps with pruned offset $offsetToPruneUpTo"
          )
        ),
      )
    }
  })

  test(
    "PRPruneACS",
    "Prune succeeds by not affecting active contracts",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, Seq(submitter))) =>
    for {
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)

      createdBefore <- participant.activeContracts(Some(Seq(submitter)))

      _ <- participant.prune(offsetToPruneUpTo)

      createdAfter <- participant.activeContracts(Some(Seq(submitter)))
    } yield {
      assert(createdBefore == createdAfter, "Pruning should not alter the set of active contracts")
    }
  })

  test(
    "PRPruneByTxId",
    "Prune succeeds as observed by individual transaction lookups",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, Seq(submitter))) =>
    for {
      offsetAndTransactionIdEntries <- populateLedgerAndGetOffsetsWithTransactionIds(
        participant,
        submitter,
      )
      offsetToPruneUpTo = offsetAndTransactionIdEntries(lastItemToPruneIndex)._1
      transactionsPerBatch = offsetAndTransactionIdEntries.size / batchesToPopulate
      prunedTransactionIds = Range(
        lastItemToPruneIndex - transactionsPerBatch + 1,
        lastItemToPruneIndex + 1,
      ).toVector.map(offsetAndTransactionIdEntries(_)._2)
      unprunedTransactionIds = Range(
        lastItemToPruneIndex + 1,
        lastItemToPruneIndex + transactionsPerBatch + 1,
      ).toVector
        .map(offsetAndTransactionIdEntries(_)._2)

      _ <- participant.prune(offsetToPruneUpTo)

      prunedTransactions <- Future.sequence(
        prunedTransactionIds.map(
          participant
            .transactionById(_, Seq(submitter), AcsDelta)
            .mustFail("attempting to read transactions before the pruning cut-off")
        )
      )

      _ <- Future.sequence(
        unprunedTransactionIds.map(participant.transactionById(_, Seq(submitter), AcsDelta))
      )
    } yield {
      prunedTransactions.foreach(
        assertGrpcError(
          _,
          RequestValidationErrors.NotFound.Update,
          Some("Update not found, or not visible."),
        )
      )
    }
  })

  test(
    "PRPruneByOffset",
    "Prune succeeds as observed by individual event lookups via transaction",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, Seq(submitter))) =>
    for {
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)
      transactionsPerBatch = offsets.size / batchesToPopulate
      prunedTransactionOffsets = Range(
        lastItemToPruneIndex - transactionsPerBatch + 1,
        lastItemToPruneIndex + 1,
      ).toVector.map(offsets(_))
      unprunedTransactionOffsets = Range(
        lastItemToPruneIndex + 1,
        lastItemToPruneIndex + transactionsPerBatch + 1,
      ).toVector
        .map(offsets(_))

      _ <- participant.prune(offsetToPruneUpTo)

      prunedFlatTransactions <- Future.sequence(
        prunedTransactionOffsets.map(
          participant
            .transactionByOffset(_, Seq(submitter), AcsDelta)
            .mustFail("attempting to read transactions before the pruning cut-off")
        )
      )

      _ <- Future.sequence(
        unprunedTransactionOffsets.map(participant.transactionByOffset(_, Seq(submitter), AcsDelta))
      )
    } yield {
      prunedFlatTransactions.foreach(
        assertGrpcError(
          _,
          RequestValidationErrors.NotFound.Update,
          Some("Update not found, or not visible."),
        )
      )
    }
  })

  test(
    "PRPruneRepeated",
    "Prune succeeds when called repeatedly",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, Seq(submitter))) =>
    for {
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)
      offsetOfFirstSurvivingTransaction = offsets(lastItemToPruneIndex + 1)

      _ <- participant.prune(offsetToPruneUpTo)

      txReqAfterPrune <- participant
        .getTransactionsRequest(participant.transactionFormat(parties = Some(Seq(submitter))))
        .map(
          _.update(
            _.beginExclusive := offsetToPruneUpTo
          )
        )
      transactionsAfterPrune <- participant.transactions(txReqAfterPrune)

      offsetAlreadyPruned = offsets(lastItemToPruneIndex / 2)

      _ <- participant.prune(offsetAlreadyPruned)

      txReqAfterRedundantPrune <- participant
        .getTransactionsRequest(participant.transactionFormat(parties = Some(Seq(submitter))))
        .map(
          _.update(
            _.beginExclusive := offsetToPruneUpTo
          )
        )
      transactionsAfterRedundantPrune <- participant.transactions(txReqAfterRedundantPrune)

      offsetToPruneUpToInSecondRealPrune = offsets((lastItemToPruneIndex + 1) * 2 - 1)
      offsetOfFirstSurvivingTransactionInSecondPrune = offsets((lastItemToPruneIndex + 1) * 2)

      // Add more events before second prune too to advance canton's safe pruning offset
      offsetsFollowingSecondRealPrune <- populateLedgerAndGetOffsets(participant, submitter)

      _ <- participant.prune(offsetToPruneUpToInSecondRealPrune)

      txReqAfterSecondPrune <- participant
        .getTransactionsRequest(participant.transactionFormat(parties = Some(Seq(submitter))))
        .map(
          _.update(
            _.beginExclusive := offsetToPruneUpToInSecondRealPrune
          )
        )
      transactionsAfterSecondPrune <- participant.transactions(txReqAfterSecondPrune)

    } yield {
      assert(
        transactionsAfterPrune.size == offsets.size - (lastItemToPruneIndex + 1),
        s"transaction count after pruning does not match expected count",
      )
      assert(
        transactionsAfterPrune.head.offset == offsetOfFirstSurvivingTransaction,
        s"transaction not pruned at expected offset",
      )
      assert(
        transactionsAfterRedundantPrune.size == offsets.size - (lastItemToPruneIndex + 1),
        s"transaction count after redundant pruning does not match expected count",
      )
      assert(
        transactionsAfterRedundantPrune.head.offset == offsetOfFirstSurvivingTransaction,
        s"transaction not pruned at expected offset after redundant prune",
      )
      assert(
        transactionsAfterSecondPrune.size == offsets.size - 2 * (lastItemToPruneIndex + 1) + offsetsFollowingSecondRealPrune.size,
        s"transaction count after second pruning does not match expected count",
      )
      assert(
        transactionsAfterSecondPrune.head.offset == offsetOfFirstSurvivingTransactionInSecondPrune,
        s"transaction not pruned at expected offset after second prune",
      )
    }
  })

  test(
    "PRPruneThenExercise",
    "Prune succeeds as observed by being able to exercise a contract created in pruned offset range",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, Seq(submitter))) =>
    for {
      createdBeforePrune <- participant.create(submitter, new model.test.Dummy(submitter))

      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)

      _ <- participant.prune(offsetToPruneUpTo)

      _ <- participant.exercise(submitter, createdBeforePrune.exerciseDummyChoice1())
    } yield ()
  })

  test(
    "PRPruneQueryEmptyRangeOk",
    // This test is not terribly useful for conformance, but helps ensure that pruning does not interfere when run before
    // TransactionServiceStreamingIT "TXEndToEnd" tests.
    "Prune succeeds and does not prevent querying empty ranges even in pruned space",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, Seq(submitter))) =>
    for {
      ledgerEnd <- participant.currentEnd()
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetInPrunedRange = offsets(lastItemToPruneIndex / 2)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)

      _ <- participant.prune(offsetToPruneUpTo)

      emptyRangeBeforePruning <- participant
        .getTransactionsRequest(
          participant.transactionFormat(parties = Some(Seq(submitter))),
          begin = ledgerEnd,
        )
        .map(_.update(_.endInclusive := ledgerEnd))

      emptyRangeInPrunedSpace <- participant
        .getTransactionsRequest(
          participant.transactionFormat(parties = Some(Seq(submitter))),
          begin = offsetInPrunedRange,
        )
        .map(
          _.update(
            _.endInclusive := offsetInPrunedRange
          )
        )

      emptyBeginLedgerEffectsWillFail <- participant.transactions(
        emptyRangeBeforePruning.update(
          _.updateFormat.includeTransactions.transactionShape := TRANSACTION_SHAPE_LEDGER_EFFECTS
        )
      )
      emptyBeginAcsDeltaWillFail <- participant.transactions(emptyRangeBeforePruning)
      emptyPrunedLedgerEffectsWillFail <- participant.transactions(
        emptyRangeInPrunedSpace.update(
          _.updateFormat.includeTransactions.transactionShape := TRANSACTION_SHAPE_LEDGER_EFFECTS
        )
      )
      emptyPrunedAcsDeltaWillFail <- participant.transactions(emptyRangeInPrunedSpace)
    } yield {
      assert(emptyBeginLedgerEffectsWillFail.isEmpty)
      assert(emptyBeginAcsDeltaWillFail.isEmpty)
      assert(emptyPrunedLedgerEffectsWillFail.isEmpty)
      assert(emptyPrunedAcsDeltaWillFail.isEmpty)
    }
  })

  test(
    "PREventsByContractIdPruned",
    "Ensure that EventsByContractId works as expected with pruned data",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, Seq(party))) =>
    def getEvents(dummyCid: model.test.Dummy.ContractId): Future[Int] = {
      val request = GetEventsByContractIdRequest(
        contractId = dummyCid.contractId,
        eventFormat = Some(participant.eventFormat(verbose = true, Some(Seq(party)))),
      )

      participant
        .getEventsByContractId(request)
        .map(r => r.created.fold(0)(_ => 1) + r.archived.fold(0)(_ => 1))
        .recover {
          case error if error.getMessage.contains("CONTRACT_EVENTS_NOT_FOUND") => 0
        }
    }

    for {
      dummyCid <- participant.create(party, new model.test.Dummy(party))
      end1 <- pruneToCurrentEnd(participant, party)
      events1 <- getEvents(dummyCid)
      exerciseCmd = participant.submitAndWaitRequest(
        party,
        dummyCid.exerciseDummyChoice1().commands,
      )
      _ <- participant.submitAndWait(exerciseCmd)
      events2 <- getEvents(dummyCid)
      _ <- participant.prune(end1)
      events3 <- getEvents(dummyCid)
      _ <- pruneToCurrentEnd(participant, party) // TODO(#16361) - This line causes the problem
      events4 <- getEvents(dummyCid)
    } yield {
      assertEquals("Expected single create event after prune", events1, 1)
      assertEquals("Expected create and consume event before prune", events2, 2)
      assertEquals(
        "Pruning to a point before create and consume does not remove events",
        events3,
        2,
      )
      assertEquals("Expected no events following prune", events4, 0)
    }
  })

  // TODO(i16065): Re-enable getEventsByContractKey tests
//  test(
//    "PREventsByContractKey",
//    "Ensure that EventsByContractKey works as expected with pruned data",
//    allocate(SingleParty),
//    runConcurrently = false,
//  )(implicit ec => { case Participants(Participant(participant, Seq(party))) =>
//    val exercisedKey = "pruning test key"
//    val key = makeTextKeyKey(party, exercisedKey)
//
//    def getEvents: Future[Int] = participant
//      .getEventsByContractKey(
//        GetEventsByContractKeyRequest(
//          contractKey = Some(key),
//          templateId = Some(Identifier.fromJavaProto(TextKey.TEMPLATE_ID.toProto)),
//          requestingParties = Seq(party),
//        )
//      )
//      .map(r => r.createEvent.fold(0)(_ => 1) + r.archiveEvent.fold(0)(_ => 1))
//
//    for {
//      textKeyCid1: TextKey.ContractId <- participant.create(
//        party,
//        new TextKey(party, exercisedKey, JList.of()),
//      )
//      _ <- pruneToCurrentEnd(participant, party).map(_.getAbsolute)
//      events1 <- getEvents
//      exerciseCmd = participant.submitAndWaitRequest(
//        party,
//        textKeyCid1.exerciseTextKeyChoice().commands,
//      )
//      _ <- participant.submitAndWaitForTransaction(exerciseCmd)
//      events2 <- getEvents
//      _ <- pruneToCurrentEnd(participant, party).map(_.getAbsolute)
//      events3 <- getEvents
//    } yield {
//      assertEquals("Expected single create event after prune", events1, 1)
//      assertEquals("Expected create and consume event before prune", events2, 2)
//      assertEquals("Expected no events following prune", events3, 0)
//    }
//  })

  private def populateLedgerAndGetOffsets(participant: ParticipantTestContext, submitter: Party)(
      implicit ec: ExecutionContext
  ): Future[Vector[Long]] =
    populateLedger(participant, submitter).map(_.map(tx => tx.offset))

  private def populateLedgerAndGetOffsetsWithTransactionIds(
      participant: ParticipantTestContext,
      submitter: Party,
  )(implicit ec: ExecutionContext): Future[Vector[(Long, String)]] =
    populateLedger(participant, submitter)
      .map(
        _.map(tx => (tx.offset, tx.updateId))
      )

  private def populateLedger(participant: ParticipantTestContext, submitter: Party)(implicit
      ec: ExecutionContext
  ): Future[Vector[Transaction]] =
    for {
      endOffsetAtTestStart <- participant.currentEnd()
      _ <- Future
        .sequence(Vector.fill(batchesToPopulate) {
          for {
            dummy <- participant.create(submitter, new model.test.Dummy(submitter))
            _ <- participant.exercise(submitter, dummy.exerciseDummyChoice1())
            _ <- participant.create(submitter, new Dummy(submitter))
          } yield ()
        })
      txReq <- participant.getTransactionsRequest(
        transactionFormat = participant.transactionFormat(
          parties = Some(Seq(submitter)),
          transactionShape = LedgerEffects,
        ),
        begin = endOffsetAtTestStart,
      )
      txs <- participant.transactions(txReq)
    } yield txs

//  // Note that the Daml template must be inspected to establish the key type and fields
//  // For the TextKey template the key is: (tkParty, tkKey) : (Party, Text)
//  // When populating the Record identifiers are not required.
//  private def makeTextKeyKey(party: Party, keyText: String) =
//    Value(
//      Value.Sum.Record(
//        Record(fields =
//          Vector(
//            RecordField(value = Some(Value(Value.Sum.Party(party)))),
//            RecordField(value = Some(Value(Value.Sum.Text(keyText)))),
//          )
//        )
//      )
//    )

  /** Note that the ledger end returned will be that prior to the dummy contract creation/prune
    * calls so will not represent the ledger end post pruning
    */
  private def pruneToCurrentEnd(participant: ParticipantTestContext, party: Party)(implicit
      ec: ExecutionContext
  ): Future[Long] =
    for {
      end <- participant.currentEnd()
      _ <- pruneCantonSafe(participant, end, party)
    } yield end

  /** We are retrying a command submission + pruning to make this test compatible with Canton.
    * That's because in Canton pruning will fail unless ACS commitments have been exchanged between
    * participants. To this end, repeatedly submitting commands is prompting Canton to exchange ACS
    * commitments and allows the pruning call to eventually succeed.
    */
  private def pruneCantonSafe(
      ledger: ParticipantTestContext,
      pruneUpTo: Long,
      party: Party,
  )(implicit ec: ExecutionContext): Future[Unit] =
    FutureAssertions.succeedsEventually(
      retryDelay = 100.millis,
      maxRetryDuration = 10.seconds,
      ledger.delayMechanism,
      "Pruning",
    ) {
      for {
        _ <- ledger.submitAndWait(
          ledger.submitAndWaitRequest(party, new Dummy(party).create.commands)
        )
        _ <- ledger.prune(pruneUpTo = pruneUpTo, attempts = 1)
      } yield ()
    }

}
