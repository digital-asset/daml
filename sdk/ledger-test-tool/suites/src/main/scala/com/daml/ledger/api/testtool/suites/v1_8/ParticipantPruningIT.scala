// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.util.regex.Pattern
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation.{Participant, _}
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.{FutureAssertions, LedgerTestSuite}
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.TransactionTree
import com.daml.ledger.api.v1.event_query_service.{
  GetEventsByContractIdRequest,
  GetEventsByContractKeyRequest,
}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.javaapi.data.Party
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.model
import com.daml.ledger.test.java.model.test.TextKey
import com.daml.ledger.test.java.semantic.divulgencetests.{
  Contract,
  DivulgeNotDiscloseTemplate,
  Divulgence,
  DivulgenceProposal,
  Dummy,
}
import com.daml.ledger.test.java.semantic.divulgencetests
import com.daml.logging.LoggingContext

import java.util.{List => JList}

import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class ParticipantPruningIT extends LedgerTestSuite {
  import CompanionImplicits._
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
  )(implicit ec => { case Participants(Participant(participant)) =>
    for {
      failure <- participant
        .prune(
          LedgerOffset(LedgerOffset.Value.Absolute("")),
          attempts = 1,
          pruneAllDivulgedContracts = true,
        )
        .mustFail("pruning without specifying an offset")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        Some("prune_up_to not specified"),
      )
    }
  })

  test(
    "PRQueryLatestPrunedOffsets",
    "It should be possible to query the latest pruned offsets",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
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

      // Prune the ledger without divulgence at firstPruningOffset
      _ <- ledger.pruneCantonSafe(
        firstPruningOffset,
        alice,
        p => new Dummy(p).create.commands,
        pruneAllDivulgedContracts = false,
      )
      (
        prunedUpToInclusive_afterFirstRegularPruning,
        allDivulgencePrunedUpToInclusive_afterFirstRegularPruning,
      ) <- ledger.latestPrunedOffsets()

      // Prune the ledger with divulgence at firstPruningOffset
      _ <- ledger.pruneCantonSafe(
        firstPruningOffset,
        alice,
        p => new Dummy(p).create.commands,
        pruneAllDivulgedContracts = true,
      )
      (
        prunedUpToInclusive_afterFirstAllDivulgencePruning,
        allDivulgencePrunedUpToInclusive_afterFirstAllDivulgencePruning,
      ) <- ledger.latestPrunedOffsets()

      // Prune the ledger with divulgence at secondPruningOffset
      _ <- ledger.pruneCantonSafe(
        secondPruningOffset,
        alice,
        p => new Dummy(p).create.commands,
        pruneAllDivulgedContracts = true,
      )
      (
        prunedUpToInclusive_afterSecondAllDivulgencePruning,
        allDivulgencePrunedUpToInclusive_afterSecondAllDivulgencePruning,
      ) <- ledger.latestPrunedOffsets()
    } yield {
      assert(
        assertion =
          prunedUpToInclusive_initial.getAbsolute < prunedUpToInclusive_afterFirstRegularPruning.getAbsolute,
        message =
          s"The initial pruning offset ($prunedUpToInclusive_initial) should be different than the latest pruning offset ($prunedUpToInclusive_afterFirstRegularPruning)",
      )

      assertEquals(
        "All divulgence pruning offset does not change after regular pruning",
        allDivulgencePrunedUpToInclusive_initial,
        allDivulgencePrunedUpToInclusive_afterFirstRegularPruning,
      )

      assertEquals(
        "Requested pruning offset matches the queried offset",
        prunedUpToInclusive_afterFirstRegularPruning,
        firstPruningOffset,
      )

      assertEquals(
        "Pruned up to inclusive offset is not changed if the same pruning offset is used",
        prunedUpToInclusive_afterFirstRegularPruning,
        prunedUpToInclusive_afterFirstAllDivulgencePruning,
      )

      assertEquals(
        "All divulgence pruning offset matches the requested pruning offset with all divulgence pruning enabled",
        allDivulgencePrunedUpToInclusive_afterFirstAllDivulgencePruning,
        firstPruningOffset,
      )

      assert(
        assertion =
          allDivulgencePrunedUpToInclusive_afterFirstAllDivulgencePruning.getAbsolute < allDivulgencePrunedUpToInclusive_afterSecondAllDivulgencePruning.getAbsolute,
        message =
          "Divulgence pruning offset advanced as well after the second prune call with all divulgence pruning enabled",
      )

      assertEquals(
        "Pruning offsets are equal after all divulgence pruning",
        allDivulgencePrunedUpToInclusive_afterSecondAllDivulgencePruning,
        prunedUpToInclusive_afterSecondAllDivulgencePruning,
      )
    }
  })

  test(
    "PRFailPruneByNonHexOffset",
    "Pruning a participant specifying a non-hexadecimal offset should fail",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(participant)) =>
    for {
      cannotPruneNonHexOffset <- participant
        .prune(
          LedgerOffset(LedgerOffset.Value.Absolute("covfefe")),
          attempts = 1,
          pruneAllDivulgedContracts = true,
        )
        .mustFail("pruning, specifying a non-hexadecimal offset")
    } yield {
      assertGrpcError(
        cannotPruneNonHexOffset,
        LedgerApiErrors.RequestValidation.NonHexOffset,
        Some("prune_up_to needs to be a hexadecimal string and not"),
      )
    }
  })

  test(
    "PRFailPruneByOutOfBoundsOffset",
    "Pruning a participant specifying an offset after the ledger end should fail",
    allocate(NoParties),
    runConcurrently =
      false, // in spite of being a negative test, cannot be run concurrently as otherwise ledger end grows
  )(implicit ec => { case Participants(Participant(participant)) =>
    for {
      actualEndExclusive <- participant.currentEnd()
      cannotPruneOffsetBeyondEnd <- participant
        .prune(actualEndExclusive, attempts = 1)
        .mustFail("pruning, specifying an offset after the ledger end")
    } yield {
      assertGrpcError(
        cannotPruneOffsetBeyondEnd,
        LedgerApiErrors.RequestValidation.OffsetOutOfRange,
        Some("prune_up_to needs to be before ledger end"),
      )
    }
  })

  test(
    "PRPruneTxTrees",
    "Prune succeeds as observed by transaction trees",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, submitter)) =>
    for {
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)
      offsetOfSecondToLastPrunedTransaction = offsets(
        lastItemToPruneIndex - 1
      ) // This offset is the largest exclusive offset we can no longer read from after
      offsetOfFirstSurvivingTransaction = offsets(lastItemToPruneIndex + 1)

      _ <- participant.prune(offsetToPruneUpTo)

      transactionsAfterPrune <- participant.transactionTrees(
        participant
          .getTransactionsRequest(participant.transactionFilter(parties = Seq(submitter)))
          .update(_.begin := offsetToPruneUpTo)
      )

      cannotReadAnymore <- participant
        .transactionTrees(
          participant
            .getTransactionsRequest(participant.transactionFilter(parties = Seq(submitter)))
            .update(_.begin := offsetOfSecondToLastPrunedTransaction)
        )
        .mustFail("attempting to read transactions before the pruning cut-off")
    } yield {
      assert(
        transactionsAfterPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
        s"transaction trees not pruned at expected offset",
      )
      assertGrpcErrorRegex(
        cannotReadAnymore,
        LedgerApiErrors.RequestValidation.ParticipantPrunedDataAccessed,
        Some(
          Pattern.compile(
            s"(Transactions request from [0-9a-fA-F]* to [0-9a-fA-F]* precedes pruned offset ${offsetToPruneUpTo.getAbsolute})|(Request from [0-9a-fA-F]* precedes pruned offset ${offsetToPruneUpTo.getAbsolute})"
          )
        ),
      )
    }
  })

  test(
    "PRPruneTxFlat",
    "Prune succeeds as observed by flat transactions",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, submitter)) =>
    for {
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)
      offsetOfSecondToLastPrunedTransaction = offsets(
        lastItemToPruneIndex - 1
      ) // This offset is the largest exclusive offset we can no longer read from after
      offsetOfFirstSurvivingTransaction = offsets(lastItemToPruneIndex + 1)

      _ <- participant.prune(offsetToPruneUpTo)

      txAfterPrune <- participant.flatTransactions(
        participant
          .getTransactionsRequest(participant.transactionFilter(parties = Seq(submitter)))
          .update(_.begin := offsetToPruneUpTo)
      )

      cannotReadAnymore <- participant
        .flatTransactions(
          participant
            .getTransactionsRequest(participant.transactionFilter(parties = Seq(submitter)))
            .update(_.begin := offsetOfSecondToLastPrunedTransaction)
        )
        .mustFail("attempting to read transactions before the pruning cut-off")
    } yield {
      assert(
        txAfterPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
        s"flat transactions not pruned at expected offset",
      )
      assertGrpcErrorRegex(
        cannotReadAnymore,
        LedgerApiErrors.RequestValidation.ParticipantPrunedDataAccessed,
        Some(
          Pattern.compile(
            s"(Transactions request from [0-9a-fA-F]* to [0-9a-fA-F]* precedes pruned offset ${offsetToPruneUpTo.getAbsolute})|(Request from [0-9a-fA-F]* precedes pruned offset ${offsetToPruneUpTo.getAbsolute})"
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
  )(implicit ec => { case Participants(Participant(participant, submitter)) =>
    for {
      endOffsetAtTestStart <- participant.currentEnd()
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)
      offsetOfSecondToLastPrunedCheckpoint = offsets(
        lastItemToPruneIndex - 1
      ) // This offset is the largest exclusive offset we can no longer read from after
      offsetOfFirstSurvivingCheckpoint = offsets(lastItemToPruneIndex + 1)

      firstCheckpointBeforePrune <- participant
        .checkpoints(1, participant.completionStreamRequest(endOffsetAtTestStart)(submitter))
        .map(_.head)

      _ <- participant.prune(offsetToPruneUpTo)

      firstCheckpointsAfterPrune <- participant
        .checkpoints(1, participant.completionStreamRequest(offsetToPruneUpTo)(submitter))
        .map(_.head)

      cannotReadAnymore <- participant
        .checkpoints(
          1,
          participant.completionStreamRequest(offsetOfSecondToLastPrunedCheckpoint)(submitter),
        )
        .mustFail("attempting to read transactions before the pruning cut-off")
    } yield {
      assert(
        firstCheckpointBeforePrune.offset.exists(o => o.getAbsolute < offsetToPruneUpTo.getAbsolute)
      )
      assert(
        firstCheckpointsAfterPrune.offset.exists(o =>
          o.getAbsolute == offsetOfFirstSurvivingCheckpoint.getAbsolute
        ),
        s"first checkpoint offset ${firstCheckpointsAfterPrune.offset} after pruning does not match expected offset $offsetOfFirstSurvivingCheckpoint",
      )
      assertGrpcErrorRegex(
        cannotReadAnymore,
        LedgerApiErrors.RequestValidation.ParticipantPrunedDataAccessed,
        Some(
          Pattern.compile(
            s"Command completions? request from [0-9a-fA-F]* to [0-9a-fA-F]* overlaps with pruned offset ${offsetToPruneUpTo.getAbsolute}"
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
  )(implicit ec => { case Participants(Participant(participant, submitter)) =>
    for {
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)

      createdBefore <- participant.activeContracts(submitter)

      _ <- participant.prune(offsetToPruneUpTo)

      createdAfter <- participant.activeContracts(submitter)
    } yield {
      assert(createdBefore == createdAfter, "Pruning should not alter the set of active contracts")
    }
  })

  test(
    "PRPruneTreeByTxId",
    "Prune succeeds as observed by individual transaction tree lookups",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, submitter)) =>
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

      prunedTransactionTrees <- Future.sequence(
        prunedTransactionIds.map(
          participant
            .transactionTreeById(_, submitter)
            .mustFail("attempting to read transactions before the pruning cut-off")
        )
      )

      _ <- Future.sequence(
        unprunedTransactionIds.map(participant.transactionTreeById(_, submitter))
      )
    } yield {
      prunedTransactionTrees.foreach(
        assertGrpcError(
          _,
          LedgerApiErrors.RequestValidation.NotFound.Transaction,
          Some("Transaction not found, or not visible."),
        )
      )
    }
  })

  test(
    "PRPruneFlatByTxId",
    "Prune succeeds as observed by individual flat transaction lookups",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, submitter)) =>
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

      prunedFlatTransactions <- Future.sequence(
        prunedTransactionIds.map(
          participant
            .flatTransactionById(_, submitter)
            .mustFail("attempting to read transactions before the pruning cut-off")
        )
      )

      _ <- Future.sequence(
        unprunedTransactionIds.map(participant.flatTransactionById(_, submitter))
      )
    } yield {
      prunedFlatTransactions.foreach(
        assertGrpcError(
          _,
          LedgerApiErrors.RequestValidation.NotFound.Transaction,
          Some("Transaction not found, or not visible."),
        )
      )
    }
  })

  test(
    "PRPruneTreeByEventId",
    "Prune succeeds as observed by individual event lookups via transaction tree",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, submitter)) =>
    for {
      offsetWithEventIdEntries <- populateLedgerAndGetOffsetsWithEventIds(participant, submitter)
      offsetToPruneUpTo = offsetWithEventIdEntries(lastItemToPruneIndex)._1
      eventsPerBatch = offsetWithEventIdEntries.size / batchesToPopulate
      prunedEventIds = Range(
        lastItemToPruneIndex - eventsPerBatch + 1,
        lastItemToPruneIndex + 1,
      ).toVector
        .map(offsetWithEventIdEntries(_)._2)
      unprunedEventIds = Range(
        lastItemToPruneIndex + 1,
        lastItemToPruneIndex + eventsPerBatch + 1,
      ).toVector
        .map(offsetWithEventIdEntries(_)._2)

      _ <- participant.prune(offsetToPruneUpTo)

      prunedEventsViaTree <- Future.sequence(
        prunedEventIds.map(
          participant
            .transactionTreeByEventId(_, submitter)
            .mustFail("attempting to read transactions before the pruning cut-off")
        )
      )

      _ <- Future.sequence(unprunedEventIds.map(participant.transactionTreeByEventId(_, submitter)))
    } yield {
      prunedEventsViaTree.foreach(
        assertGrpcError(
          _,
          LedgerApiErrors.RequestValidation.NotFound.Transaction,
          Some("Transaction not found, or not visible."),
        )
      )
    }
  })

  test(
    "PRPruneFlatByEventId",
    "Prune succeeds as observed by individual event lookups via flat transaction",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, submitter)) =>
    for {
      offsetWithEventIdEntries <- populateLedgerAndGetOffsetsWithEventIds(participant, submitter)
      offsetToPruneUpTo = offsetWithEventIdEntries(lastItemToPruneIndex)._1
      eventsPerBatch = offsetWithEventIdEntries.size / batchesToPopulate
      prunedEventIds = Range(
        lastItemToPruneIndex - eventsPerBatch + 1,
        lastItemToPruneIndex + 1,
      ).toVector
        .map(offsetWithEventIdEntries(_)._2)
      unprunedEventIds = Range(
        lastItemToPruneIndex + 1,
        lastItemToPruneIndex + eventsPerBatch + 1,
      ).toVector
        .map(offsetWithEventIdEntries(_)._2)

      _ <- participant.prune(offsetToPruneUpTo)

      prunedEventsViaFlat <- Future.sequence(
        prunedEventIds.map(
          participant
            .flatTransactionByEventId(_, submitter)
            .mustFail("attempting to read transactions before the pruning cut-off")
        )
      )

      _ <- Future.sequence(unprunedEventIds.map(participant.flatTransactionByEventId(_, submitter)))
    } yield {
      prunedEventsViaFlat.foreach(
        assertGrpcError(
          _,
          LedgerApiErrors.RequestValidation.NotFound.Transaction,
          Some("Transaction not found, or not visible."),
        )
      )
    }
  })

  test(
    "PRPruneRepeated",
    "Prune succeeds when called repeatedly",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, submitter)) =>
    for {
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)
      offsetOfFirstSurvivingTransaction = offsets(lastItemToPruneIndex + 1)

      _ <- participant.prune(offsetToPruneUpTo)

      transactionsAfterPrune <- participant.transactionTrees(
        participant
          .getTransactionsRequest(participant.transactionFilter(parties = Seq(submitter)))
          .update(_.begin := offsetToPruneUpTo)
      )

      offsetAlreadyPruned = offsets(lastItemToPruneIndex / 2)

      _ <- participant.prune(offsetAlreadyPruned)

      transactionsAfterRedundantPrune <- participant.transactionTrees(
        participant
          .getTransactionsRequest(participant.transactionFilter(parties = Seq(submitter)))
          .update(_.begin := offsetToPruneUpTo)
      )

      offsetToPruneUpToInSecondRealPrune = offsets((lastItemToPruneIndex + 1) * 2 - 1)
      offsetOfFirstSurvivingTransactionInSecondPrune = offsets((lastItemToPruneIndex + 1) * 2)

      // Add more events before second prune too to advance canton's safe pruning offset
      offsetsFollowingSecondRealPrune <- populateLedgerAndGetOffsets(participant, submitter)

      _ <- participant.prune(offsetToPruneUpToInSecondRealPrune)

      transactionsAfterSecondPrune <- participant.transactionTrees(
        participant
          .getTransactionsRequest(participant.transactionFilter(parties = Seq(submitter)))
          .update(_.begin := offsetToPruneUpToInSecondRealPrune)
      )

    } yield {
      assert(
        transactionsAfterPrune.size == offsets.size - (lastItemToPruneIndex + 1),
        s"transaction tree count after pruning does not match expected count",
      )
      assert(
        transactionsAfterPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
        s"transaction trees not pruned at expected offset",
      )
      assert(
        transactionsAfterRedundantPrune.size == offsets.size - (lastItemToPruneIndex + 1),
        s"transaction tree count after redundant pruning does not match expected count",
      )
      assert(
        transactionsAfterRedundantPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
        s"transaction trees not pruned at expected offset after redundant prune",
      )
      assert(
        transactionsAfterSecondPrune.size == offsets.size - 2 * (lastItemToPruneIndex + 1) + offsetsFollowingSecondRealPrune.size,
        s"transaction tree count after second pruning does not match expected count",
      )
      assert(
        transactionsAfterSecondPrune.head.offset == offsetOfFirstSurvivingTransactionInSecondPrune.getAbsolute,
        s"transaction trees not pruned at expected offset after second prune",
      )
    }
  })

  test(
    "PRPruneThenExercise",
    "Prune succeeds as observed by being able to exercise a contract created in pruned offset range",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, submitter)) =>
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
    // TransactionServiceStreamingIT "TXBeginToBegin" and "TXTreesBeginToBegin" tests.
    "Prune succeeds and does not prevent querying empty ranges even in pruned space",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, submitter)) =>
    for {
      offsets <- populateLedgerAndGetOffsets(participant, submitter)
      offsetInPrunedRange = offsets(lastItemToPruneIndex / 2)
      offsetToPruneUpTo = offsets(lastItemToPruneIndex)

      _ <- participant.prune(offsetToPruneUpTo)

      emptyRangeAtBegin = participant
        .getTransactionsRequest(
          participant.transactionFilter(parties = Seq(submitter)),
          begin = participant.begin,
        )
        .update(_.end := participant.begin)

      emptyRangeInPrunedSpace = participant
        .getTransactionsRequest(
          participant.transactionFilter(parties = Seq(submitter)),
          begin = offsetInPrunedRange,
        )
        .update(_.end := offsetInPrunedRange)

      emptyBeginTreesWillFail <- participant.transactionTrees(emptyRangeAtBegin)
      emptyBeginFlatWillFail <- participant.flatTransactions(emptyRangeAtBegin)
      emptyPrunedTreesWillFail <- participant.transactionTrees(emptyRangeInPrunedSpace)
      emptyPrunedFlatWillFail <- participant.flatTransactions(emptyRangeInPrunedSpace)
    } yield {
      assert(emptyBeginTreesWillFail.isEmpty)
      assert(emptyBeginFlatWillFail.isEmpty)
      assert(emptyPrunedTreesWillFail.isEmpty)
      assert(emptyPrunedFlatWillFail.isEmpty)
    }
  })

  test(
    "PRDivulgenceArchivalPruning",
    "Prune succeeds for divulgence events whose contracts are archived",
    allocate(TwoParties),
    runConcurrently = false, // pruning call may interact with other tests
  )(implicit ec => { case Participants(Participant(participant, alice, bob)) =>
    for {
      divulgence <- createDivulgence(alice, bob, participant, participant)
      contract <- participant.create(alice, new Contract(alice))

      // Retroactively divulge Alice's contract to bob
      _ <- participant.exercise(alice, divulgence.exerciseDivulge(contract))

      // Bob can see the divulged contract
      _ <- participant
        .exerciseAndGetContract[divulgencetests.Dummy.ContractId, divulgencetests.Dummy](
          bob,
          divulgence.exerciseCanFetch(contract),
        )

      _ <- pruneAtCurrentOffset(
        participant,
        bob,
        pruneAllDivulgedContracts = false,
      )

      // Bob can still see the divulged contract
      _ <- participant
        .exerciseAndGetContract[divulgencetests.Dummy.ContractId, divulgencetests.Dummy](
          bob,
          divulgence.exerciseCanFetch(contract),
        )

      // Archive the divulged contract
      _ <- participant.exercise(alice, contract.exerciseArchive())

      _ <- pruneAtCurrentOffset(
        participant,
        bob,
        pruneAllDivulgedContracts = false,
      )

      _ <- participant
        .exerciseAndGetContract[divulgencetests.Dummy.ContractId, divulgencetests.Dummy](
          bob,
          divulgence.exerciseCanFetch(contract),
        )
        .mustFailWith("Bob cannot access a divulged contract which was already archived") {
          exception =>
            val errorMessage = exception.getMessage
            errorMessage.contains(
              "Contract could not be found with id"
            ) && errorMessage.contains(contract.contractId)
        }
    } yield ()
  })

  test(
    "PRRetroactiveDivulgences",
    "Divulgence pruning succeeds",
    allocate(SingleParty, SingleParty),
    runConcurrently = false, // pruning call may interact with other tests
    // Higher timeout - The test generates a significant number of events
    timeoutScale = 8.0,
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      divulgence <- createDivulgence(alice, bob, alpha, beta)
      contract <- alpha.create(alice, new Contract(alice))

      _ <- synchronize(alpha, beta) // because of exercise on beta inside createDivulgence

      // Retroactively divulge Alice's contract to bob
      _ <- alpha.exercise(
        alice,
        divulgence.exerciseDivulge(contract),
      )

      _ <- divulgencePruneAndCheck(alice, bob, alpha, beta, contract, divulgence)
    } yield ()
  })

  test(
    "PRLocalAndNonLocalRetroactiveDivulgences",
    "Divuglence pruning succeeds if first divulgence is not a disclosure but happens in the same transaction as the create",
    allocate(SingleParty, SingleParty),
    runConcurrently = false, // pruning call may interact with other tests
    // Higher timeout - The test generates a significant number of events
    timeoutScale = 8.0,
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      divulgence <- createDivulgence(alice, bob, alpha, beta)

      divulgeNotDiscloseTemplate <- alpha.create(alice, new DivulgeNotDiscloseTemplate(alice, bob))(
        DivulgeNotDiscloseTemplate.COMPANION
      )

      // Alice creates contract in a context not visible to Bob and follows with a divulgence to Bob in the same transaction
      contract <- alpha.exerciseAndGetContractNoDisclose[Contract.ContractId](
        alice,
        divulgeNotDiscloseTemplate.exerciseDivulgeNoDisclose(divulgence),
      )

      _ <- divulgencePruneAndCheck(alice, bob, alpha, beta, contract, divulgence)
    } yield ()
  })

  // This test can only be run in multi-participant setups, since the participant
  // on which we assert pruning of the immediate divulgence
  // must not have a locally-hosted stakeholder of the divulged contract.
  test(
    "PRImmediateAndRetroactiveDivulgence",
    "Immediate divulgence pruning succeeds",
    allocate(SingleParty, SingleParty).expectingMinimumActualParticipantCount(2),
    runConcurrently = false, // pruning call may interact with other tests
    // Higher timeout - The test generates a significant number of events
    timeoutScale = 8.0,
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      divulgence <- createDivulgence(alice, bob, alpha, beta)
      // synchronize to wait until alice has observed contract divulged by bob
      _ <- synchronize(alpha, beta)
      // Alice's contract creation is disclosed to Bob
      contract <- alpha.exerciseAndGetContract[Contract.ContractId, Contract](
        alice,
        divulgence.exerciseCreateAndDisclose(),
      )

      _ <- divulgencePruneAndCheck(alice, bob, alpha, beta, contract, divulgence)
    } yield ()
  })

  test(
    "PREventsByContractIdPruned",
    "Ensure that EventsByContractId works as expected with pruned data",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, party)) =>
    def getEvents(dummyCid: model.test.Dummy.ContractId): Future[Int] = {
      val request = GetEventsByContractIdRequest(dummyCid.contractId, Seq(party))
      participant
        .getEventsByContractId(request)
        .map(r => r.createEvent.fold(0)(_ => 1) + r.archiveEvent.fold(0)(_ => 1))
    }

    for {
      dummyCid <- participant.create(party, new model.test.Dummy(party))
      end1 <- pruneToCurrentEnd(participant, party)
      events1 <- getEvents(dummyCid)
      exerciseCmd = participant.submitAndWaitRequest(
        party,
        dummyCid.exerciseDummyChoice1().commands,
      )
      _ <- participant.submitAndWaitForTransactionTree(exerciseCmd)
      events2 <- getEvents(dummyCid)
      _ <- participant.prune(end1)
      events3 <- getEvents(dummyCid)
      _ <- pruneToCurrentEnd(participant, party) // TODO - This line causes the problem
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

  test(
    "PREventsByContractKey",
    "Ensure that EventsByContractKey works as expected with pruned data",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, party)) =>
    val exercisedKey = "pruning test key"
    val key = makeTextKeyKey(party, exercisedKey)

    def getEvents: Future[Int] = participant
      .getEventsByContractKey(
        GetEventsByContractKeyRequest(
          contractKey = Some(key),
          templateId = Some(Identifier.fromJavaProto(TextKey.TEMPLATE_ID.toProto)),
          requestingParties = Seq(party),
        )
      )
      .map(r => r.createEvent.fold(0)(_ => 1) + r.archiveEvent.fold(0)(_ => 1))

    for {
      textKeyCid1: TextKey.ContractId <- participant.create(
        party,
        new TextKey(party, exercisedKey, JList.of()),
      )
      _ <- pruneToCurrentEnd(participant, party).map(_.getAbsolute)
      events1 <- getEvents
      exerciseCmd = participant.submitAndWaitRequest(
        party,
        textKeyCid1.exerciseTextKeyChoice().commands,
      )
      _ <- participant.submitAndWaitForTransactionTree(exerciseCmd)
      events2 <- getEvents
      _ <- pruneToCurrentEnd(participant, party).map(_.getAbsolute)
      events3 <- getEvents
    } yield {
      assertEquals("Expected single create event after prune", events1, 1)
      assertEquals("Expected create and consume event before prune", events2, 2)
      assertEquals("Expected no events following prune", events3, 0)
    }
  })

  test(
    "PRTPFailToRequestPruningOffsetsWithEnd",
    "Pruning offsets should not be served on streams with end specified",
    allocate(SingleParty),
    enabled = _.prunedOffsets,
    disabledReason = "Ledger does not support pruned offset streaming",
  )(implicit ec => {
    case Participants(Participant(ledger, party)) => {
      val req = ledger
        .getTransactionsRequest(ledger.transactionFilter(parties = Seq(party)))
        .copy(sendPrunedOffsets = true)

      for {
        flatFailure <- ledger
          .rawFlatTransactions(1, req)
          .mustFail("subscribing past the ledger end")
        treeFailure <- ledger
          .rawTransactionTrees(1, req)
          .mustFail("subscribing past the ledger end")
      } yield {
        assertGrpcError(
          flatFailure,
          LedgerApiErrors.RequestValidation.InvalidArgument,
          Some("Pruning offsets requested on a stream with an explicit end"),
        )
        assertGrpcError(
          treeFailure,
          LedgerApiErrors.RequestValidation.InvalidArgument,
          Some("Pruning offsets requested on a stream with an explicit end"),
        )
      }
    }
  })

  test(
    "PRTServePruningOffsets",
    "Pruning offsets should be served when requested",
    allocate(TwoParties),
    enabled = _.prunedOffsets,
    disabledReason = "Ledger does not support pruned offset streaming",
    runConcurrently = false,
  )(implicit ec => {
    case Participants(Participant(ledger, party, party2)) => {
      val expectedMsgs = 11

      def validate[Response](
          transactions: Vector[Response],
          extractOffset: Response => String,
          offsetToPruneUpTo: LedgerOffset,
      ): Unit = {
        assert(
          transactions.size == expectedMsgs,
          s"$expectedMsgs should have been submitted but ${transactions.size} were instead",
        )
        val expectedOffset = offsetToPruneUpTo.value.absolute.fold("")(identity)
        assert(
          transactions.exists(extractOffset(_) == expectedOffset),
          s"pruning offset of $expectedOffset should have been received",
        )
      }

      val req = ledger
        .getTransactionsRequest(ledger.transactionFilter(parties = Seq(party)))
        .copy(sendPrunedOffsets = true)
        .copy(end = None)
      val flatsF = ledger.rawFlatTransactions(expectedMsgs, req)
      val treesF = ledger.rawTransactionTrees(expectedMsgs, req)
      for {
        offsetToPruneUpTo <- ledger.currentEnd()
        _ <- Future.sequence(
          Vector.fill((expectedMsgs - 1) / 2)(ledger.create(party, new Dummy(party)))
        )
        _ <- pruneCantonSafe(ledger, offsetToPruneUpTo, party2)
        _ <- Future.sequence(
          Vector.fill((expectedMsgs - 1) / 2)(ledger.create(party, new Dummy(party)))
        )
        flats <- flatsF
        trees <- treesF

      } yield {
        validate[GetTransactionsResponse](flats, _.prunedOffset, offsetToPruneUpTo)
        validate[GetTransactionTreesResponse](trees, _.prunedOffset, offsetToPruneUpTo)
      }
    }
  })

  test(
    "PRTDontServePruningOffsets",
    "Pruning offsets should not be served when not requested",
    allocate(TwoParties),
    enabled = _.prunedOffsets,
    disabledReason = "Ledger does not support pruned offset streaming",
    runConcurrently = false,
  )(implicit ec => {
    case Participants(Participant(ledger, party, party2)) => {
      val expectedMsgs = 10

      def validate[Response](
          transactions: Vector[Response],
          validateNoOffset: Response => Boolean,
      ): Unit = {
        assert(
          transactions.size == expectedMsgs,
          s"$expectedMsgs should have been submitted but ${transactions.size} were instead",
        )
        assert(
          transactions.forall(validateNoOffset),
          s"pruning offset was received",
        )
      }

      val req = ledger
        .getTransactionsRequest(ledger.transactionFilter(parties = Seq(party)))
        .copy(sendPrunedOffsets = false)
        .copy(end = None)
      val flatsF = ledger.rawFlatTransactions(expectedMsgs, req)
      val treesF = ledger.rawTransactionTrees(expectedMsgs, req)
      for {
        offsetToPruneUpTo <- ledger.currentEnd()
        _ <- Future.sequence(
          Vector.fill(expectedMsgs / 2)(ledger.create(party, new Dummy(party)))
        )
        _ <- pruneCantonSafe(ledger, offsetToPruneUpTo, party2)
        _ <- Future.sequence(
          Vector.fill(expectedMsgs / 2)(ledger.create(party, new Dummy(party)))
        )
        flats <- flatsF
        trees <- treesF

      } yield {
        validate[GetTransactionsResponse](flats, _.prunedOffset.isEmpty)
        validate[GetTransactionTreesResponse](trees, _.prunedOffset.isEmpty)
      }
    }
  })

  private def createDivulgence(
      alice: Party,
      bob: Party,
      alpha: ParticipantTestContext,
      beta: ParticipantTestContext,
  )(implicit ec: ExecutionContext) =
    for {
      divulgenceHelper <- alpha.create(alice, new DivulgenceProposal(alice, bob))(
        DivulgenceProposal.COMPANION
      )
      _ <- synchronize(alpha, beta)
      divulgence <- beta.exerciseAndGetContract[Divulgence.ContractId, Divulgence](
        bob,
        divulgenceHelper.exerciseAccept(),
      )(Divulgence.COMPANION)
    } yield divulgence

  private def divulgencePruneAndCheck(
      alice: Party,
      bob: Party,
      alpha: ParticipantTestContext,
      beta: ParticipantTestContext,
      contract: Contract.ContractId,
      divulgence: Divulgence.ContractId,
  )(implicit ec: ExecutionContext) =
    for {
      _ <- synchronize(alpha, beta)

      // Check that Bob can fetch the contract
      _ <- beta.exerciseAndGetContract[divulgencetests.Dummy.ContractId, divulgencetests.Dummy](
        bob,
        divulgence.exerciseCanFetch(contract),
      )

      offsetAfterDivulgence_1 <- beta.currentEnd()

      // Alice re-divulges the contract to Bob
      _ <- alpha.exerciseAndGetContractNoDisclose[divulgencetests.Contract.ContractId](
        alice,
        divulgence.exerciseDivulge(contract),
      )

      _ <- synchronize(alpha, beta)

      // Check that Bob can fetch the contract
      _ <- beta.exerciseAndGetContract[divulgencetests.Dummy.ContractId, divulgencetests.Dummy](
        bob,
        divulgence.exerciseCanFetch(contract),
      )

      // Add events to both participants to advance canton's safe pruning offset
      _ <- populateLedgerAndGetOffsets(alpha, alice)
      _ <- populateLedgerAndGetOffsets(beta, bob)

      _ <- beta.prune(
        pruneUpTo = offsetAfterDivulgence_1,
        pruneAllDivulgedContracts = true,
      )
      // Check that Bob can still fetch the contract after pruning the first transaction
      _ <- beta.exerciseAndGetContract[divulgencetests.Dummy.ContractId, divulgencetests.Dummy](
        bob,
        divulgence.exerciseCanFetch(contract),
      )

      // Populate "other" participant too to advance canton's safe pruning offset
      _ <- populateLedgerAndGetOffsets(alpha, alice)

      _ <- pruneAtCurrentOffset(beta, bob, pruneAllDivulgedContracts = true)

      _ <- beta
        .exerciseAndGetContract[divulgencetests.Dummy.ContractId, divulgencetests.Dummy](
          bob,
          divulgence.exerciseCanFetch(contract),
        )
        .mustFail("Bob cannot access the divulged contract after the second pruning")
    } yield ()

  private def populateLedgerAndGetOffsets(participant: ParticipantTestContext, submitter: Party)(
      implicit ec: ExecutionContext
  ): Future[Vector[LedgerOffset]] =
    populateLedger(participant, submitter)
      .map(_.map(tree => LedgerOffset.of(LedgerOffset.Value.Absolute(tree.offset))))

  private def populateLedgerAndGetOffsetsWithTransactionIds(
      participant: ParticipantTestContext,
      submitter: Party,
  )(implicit ec: ExecutionContext): Future[Vector[(LedgerOffset, String)]] =
    populateLedger(participant, submitter)
      .map(
        _.map(tree =>
          (LedgerOffset.of(LedgerOffset.Value.Absolute(tree.offset)), tree.transactionId)
        )
      )

  private def populateLedgerAndGetOffsetsWithEventIds(
      participant: ParticipantTestContext,
      submitter: Party,
  )(implicit ec: ExecutionContext): Future[Vector[(LedgerOffset, String)]] =
    populateLedger(participant, submitter)
      .map(
        _.map(tree =>
          (LedgerOffset.of(LedgerOffset.Value.Absolute(tree.offset)), tree.eventsById.keys.head)
        )
      )

  private def populateLedger(participant: ParticipantTestContext, submitter: Party)(implicit
      ec: ExecutionContext
  ): Future[Vector[TransactionTree]] =
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
      trees <- participant.transactionTrees(
        participant.getTransactionsRequest(
          participant.transactionFilter(parties = Seq(submitter)),
          begin = endOffsetAtTestStart,
        )
      )
    } yield trees

  private def pruneAtCurrentOffset(
      participant: ParticipantTestContext,
      localParty: Party,
      pruneAllDivulgedContracts: Boolean,
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      offset <- participant.currentEnd()

      _ <- populateLedgerAndGetOffsets(participant, localParty)

      // Dummy needed to prune at this offset
      _ <- participant.create(localParty, new Dummy(localParty))

      acsBeforePruning <- participant.activeContracts(localParty)
      _ <- participant.prune(offset, pruneAllDivulgedContracts = pruneAllDivulgedContracts)
      acsAfterPruning <- participant.activeContracts(localParty)

    } yield {
      assert(
        acsBeforePruning == acsAfterPruning,
        s"Active contract set comparison before and after pruning failed: $acsBeforePruning vs $acsAfterPruning",
      )
    }

  // Note that the Daml template must be inspected to establish the key type and fields
  // For the TextKey template the key is: (tkParty, tkKey) : (Party, Text)
  // When populating the Record identifiers are not required.
  private def makeTextKeyKey(party: Party, keyText: String) = {
    Value(
      Value.Sum.Record(
        Record(fields =
          Vector(
            RecordField(value = Some(Value(Value.Sum.Party(party)))),
            RecordField(value = Some(Value(Value.Sum.Text(keyText)))),
          )
        )
      )
    )
  }

  /** Note that the ledger end returned will be that prior to the dummy contract creation/prune calls
    * so will not represent the ledger end post pruning
    */
  private def pruneToCurrentEnd(participant: ParticipantTestContext, party: Party)(implicit
      ec: ExecutionContext
  ): Future[LedgerOffset] = {
    for {
      end <- participant.currentEnd()
      _ <- pruneCantonSafe(participant, end, party)
    } yield end
  }

  /** We are retrying a command submission + pruning to make this test compatible with Canton.
    * That's because in Canton pruning will fail unless ACS commitments have been exchanged between participants.
    * To this end, repeatedly submitting commands is prompting Canton to exchange ACS commitments
    * and allows the pruning call to eventually succeed.
    */
  private def pruneCantonSafe(
      ledger: ParticipantTestContext,
      pruneUpTo: LedgerOffset,
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
