// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.regex.Pattern

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.TransactionTree
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.model.Test.Dummy
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class ParticipantPruningIT extends LedgerTestSuite {

  private val batchesToPopulate = 74
  private val lastItemToPruneIndex = batchesToPopulate

  test(
    "PRFailPruneByNoOffset",
    "Pruning a participant without specifying an offset should fail",
    allocate(NoParties))(implicit ec => {
    case Participants(Participant(participant)) =>
      for {
        failure <- participant.prune("", attempts = 1).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "prune_up_to not specified")
      }
  })

  test(
    "PRFailPruneByNonHexOffset",
    "Pruning a participant specifying a non-hexadecimal offset should fail",
    allocate(NoParties))(implicit ec => {
    case Participants(Participant(participant)) =>
      for {
        cannotPruneNonHexOffset <- participant.prune("cofefe", attempts = 1).failed
      } yield {
        assertGrpcError(
          cannotPruneNonHexOffset,
          Status.Code.INVALID_ARGUMENT,
          "prune_up_to needs to be a hexadecimal string and not")
      }
  })

  test(
    "PRFailPruneByOutOfBoundsOffset",
    "Pruning a participant specifying an offset after the ledger end should fail",
    allocate(NoParties),
    runConcurrently = false // in spite of being a negative test, cannot be run concurrently as otherwise ledger end grows
  )(implicit ec => {
    case Participants(Participant(participant)) =>
      for {
        actualEndExclusive <- participant.currentEnd()
        cannotPruneOffsetBeyondEnd <- participant
          .prune(actualEndExclusive, attempts = 1)
          .failed
      } yield {
        assertGrpcError(
          cannotPruneOffsetBeyondEnd,
          Status.Code.INVALID_ARGUMENT,
          "prune_up_to needs to be before ledger end")
      }
  })

  test(
    "PRPruneTxTrees",
    "Prune succeeds as observed by transaction trees",
    allocate(SingleParty),
    runConcurrently = false)(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        offsets <- populateLedgerAndGetOffsets(participant, submitter)
        offsetToPruneUpTo = offsets(lastItemToPruneIndex)
        offsetOfSecondToLastPrunedTransaction = offsets(lastItemToPruneIndex - 1) // This offset is the largest exclusive offset we can no longer read from after
        offsetOfFirstSurvivingTransaction = offsets(lastItemToPruneIndex + 1)

        _ <- participant.prune(offsetToPruneUpTo)

        transactionsAfterPrune <- participant.transactionTrees(
          participant
            .getTransactionsRequest(parties = Seq(submitter))
            .update(_.begin := offsetToPruneUpTo))

        cannotReadAnymore <- participant
          .transactionTrees(
            participant
              .getTransactionsRequest(parties = Seq(submitter))
              .update(_.begin := offsetOfSecondToLastPrunedTransaction)
          )
          .failed
      } yield {
        assert(
          transactionsAfterPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
          s"transaction trees not pruned at expected offset"
        )
        assertGrpcError(
          cannotReadAnymore,
          Status.Code.NOT_FOUND,
          Some(Pattern.compile(
            s"(Transactions request from [0-9a-fA-F]* to [0-9a-fA-F]* precedes pruned offset ${offsetToPruneUpTo.getAbsolute})|(Request from [0-9a-fA-F]* precedes pruned offset ${offsetToPruneUpTo.getAbsolute})"))
        )
      }
  })

  test(
    "PRPruneTxFlat",
    "Prune succeeds as observed by flat transactions",
    allocate(SingleParty),
    runConcurrently = false)(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        offsets <- populateLedgerAndGetOffsets(participant, submitter)
        offsetToPruneUpTo = offsets(lastItemToPruneIndex)
        offsetOfSecondToLastPrunedTransaction = offsets(lastItemToPruneIndex - 1) // This offset is the largest exclusive offset we can no longer read from after
        offsetOfFirstSurvivingTransaction = offsets(lastItemToPruneIndex + 1)

        _ <- participant.prune(offsetToPruneUpTo)

        txAfterPrune <- participant.flatTransactions(
          participant
            .getTransactionsRequest(parties = Seq(submitter))
            .update(_.begin := offsetToPruneUpTo))

        cannotReadAnymore <- participant
          .flatTransactions(
            participant
              .getTransactionsRequest(parties = Seq(submitter))
              .update(_.begin := offsetOfSecondToLastPrunedTransaction)
          )
          .failed
      } yield {
        assert(
          txAfterPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
          s"flat transactions not pruned at expected offset"
        )
        assertGrpcError(
          cannotReadAnymore,
          Status.Code.NOT_FOUND,
          Some(Pattern.compile(
            s"(Transactions request from [0-9a-fA-F]* to [0-9a-fA-F]* precedes pruned offset ${offsetToPruneUpTo.getAbsolute})|(Request from [0-9a-fA-F]* precedes pruned offset ${offsetToPruneUpTo.getAbsolute})"))
        )
      }
  })

  test(
    "PRPruneCompletions",
    "Prune succeeds as observed by command completions",
    allocate(SingleParty),
    runConcurrently = false)(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        endOffsetAtTestStart <- participant.currentEnd()
        offsets <- populateLedgerAndGetOffsets(participant, submitter)
        offsetToPruneUpTo = offsets(lastItemToPruneIndex)
        offsetOfSecondToLastPrunedCheckpoint = offsets(lastItemToPruneIndex - 1) // This offset is the largest exclusive offset we can no longer read from after
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
            participant.completionStreamRequest(offsetOfSecondToLastPrunedCheckpoint)(submitter))
          .failed
      } yield {
        assert(firstCheckpointBeforePrune.offset.exists(o =>
          o.getAbsolute < offsetToPruneUpTo.getAbsolute))
        assert(
          firstCheckpointsAfterPrune.offset.exists(o =>
            o.getAbsolute == offsetOfFirstSurvivingCheckpoint.getAbsolute),
          s"first checkpoint offset ${firstCheckpointsAfterPrune.offset} after pruning does not match expected offset ${offsetOfFirstSurvivingCheckpoint}"
        )
        assertGrpcError(
          cannotReadAnymore,
          Status.Code.NOT_FOUND,
          Some(Pattern.compile(
            s"Command completions? request from [0-9a-fA-F]* to [0-9a-fA-F]* overlaps with pruned offset ${offsetToPruneUpTo.getAbsolute}"))
        )

      }
  })

  test(
    "PRPruneACS",
    "Prune succeeds by not affecting active contracts",
    allocate(SingleParty),
    runConcurrently = false)(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        offsets <- populateLedgerAndGetOffsets(participant, submitter)
        offsetToPruneUpTo = offsets(lastItemToPruneIndex)

        createdBefore <- participant.activeContracts(submitter)

        _ <- participant.prune(offsetToPruneUpTo)

        createdAfter <- participant.activeContracts(submitter)
      } yield {
        assert(
          createdBefore == createdAfter,
          "Pruning should not alter the set of active contracts")
      }
  })

  test(
    "PRPruneTreeByTxId",
    "Prune succeeds as observed by individual transaction tree lookups",
    allocate(SingleParty),
    runConcurrently = false)(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        offsetAndTransactionIdEntries <- populateLedgerAndGetOffsetsWithTransactionIds(
          participant,
          submitter)
        offsetToPruneUpTo = offsetAndTransactionIdEntries(lastItemToPruneIndex)._1
        transactionsPerBatch = offsetAndTransactionIdEntries.size / batchesToPopulate
        prunedTransactionIds = Range(
          lastItemToPruneIndex - transactionsPerBatch + 1,
          lastItemToPruneIndex + 1).toVector.map(offsetAndTransactionIdEntries(_)._2)
        unprunedTransactionIds = Range(
          lastItemToPruneIndex + 1,
          lastItemToPruneIndex + transactionsPerBatch + 1).toVector
          .map(offsetAndTransactionIdEntries(_)._2)

        _ <- participant.prune(offsetToPruneUpTo)

        prunedTransactionTrees <- Future.sequence(
          prunedTransactionIds.map(participant.transactionTreeById(_, submitter).failed))

        _ <- Future.sequence(
          unprunedTransactionIds.map(participant.transactionTreeById(_, submitter)))
      } yield {
        prunedTransactionTrees.foreach(
          assertGrpcError(_, Status.Code.NOT_FOUND, "Transaction not found, or not visible."))
      }
  })

  test(
    "PRPruneFlatByTxId",
    "Prune succeeds as observed by individual flat transaction lookups",
    allocate(SingleParty),
    runConcurrently = false)(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        offsetAndTransactionIdEntries <- populateLedgerAndGetOffsetsWithTransactionIds(
          participant,
          submitter)
        offsetToPruneUpTo = offsetAndTransactionIdEntries(lastItemToPruneIndex)._1
        transactionsPerBatch = offsetAndTransactionIdEntries.size / batchesToPopulate
        prunedTransactionIds = Range(
          lastItemToPruneIndex - transactionsPerBatch + 1,
          lastItemToPruneIndex + 1).toVector.map(offsetAndTransactionIdEntries(_)._2)
        unprunedTransactionIds = Range(
          lastItemToPruneIndex + 1,
          lastItemToPruneIndex + transactionsPerBatch + 1).toVector
          .map(offsetAndTransactionIdEntries(_)._2)

        _ <- participant.prune(offsetToPruneUpTo)

        prunedFlatTransactions <- Future.sequence(
          prunedTransactionIds.map(participant.flatTransactionById(_, submitter).failed))

        _ <- Future.sequence(
          unprunedTransactionIds.map(participant.flatTransactionById(_, submitter)))
      } yield {
        prunedFlatTransactions.foreach(
          assertGrpcError(_, Status.Code.NOT_FOUND, "Transaction not found, or not visible."))
      }
  })

  test(
    "PRPruneTreeByEventId",
    "Prune succeeds as observed by individual event lookups via transaction tree",
    allocate(SingleParty),
    runConcurrently = false)(
    implicit ec => {
      case Participants(Participant(participant, submitter)) =>
        for {
          offsetWithEventIdEntries <- populateLedgerAndGetOffsetsWithEventIds(
            participant,
            submitter)
          offsetToPruneUpTo = offsetWithEventIdEntries(lastItemToPruneIndex)._1
          eventsPerBatch = offsetWithEventIdEntries.size / batchesToPopulate
          prunedEventIds = Range(
            lastItemToPruneIndex - eventsPerBatch + 1,
            lastItemToPruneIndex + 1).toVector
            .map(offsetWithEventIdEntries(_)._2)
          unprunedEventIds = Range(
            lastItemToPruneIndex + 1,
            lastItemToPruneIndex + eventsPerBatch + 1).toVector
            .map(offsetWithEventIdEntries(_)._2)

          _ <- participant.prune(offsetToPruneUpTo)

          prunedEventsViaTree <- Future.sequence(
            prunedEventIds.map(participant.transactionTreeByEventId(_, submitter).failed))

          _ <- Future.sequence(
            unprunedEventIds.map(participant.transactionTreeByEventId(_, submitter)))
        } yield {
          prunedEventsViaTree.foreach(
            assertGrpcError(_, Status.Code.NOT_FOUND, "Transaction not found, or not visible."))
        }
    })

  test(
    "PRPruneFlatByEventId",
    "Prune succeeds as observed by individual event lookups via flat transaction",
    allocate(SingleParty),
    runConcurrently = false)(
    implicit ec => {
      case Participants(Participant(participant, submitter)) =>
        for {
          offsetWithEventIdEntries <- populateLedgerAndGetOffsetsWithEventIds(
            participant,
            submitter)
          offsetToPruneUpTo = offsetWithEventIdEntries(lastItemToPruneIndex)._1
          eventsPerBatch = offsetWithEventIdEntries.size / batchesToPopulate
          prunedEventIds = Range(
            lastItemToPruneIndex - eventsPerBatch + 1,
            lastItemToPruneIndex + 1).toVector
            .map(offsetWithEventIdEntries(_)._2)
          unprunedEventIds = Range(
            lastItemToPruneIndex + 1,
            lastItemToPruneIndex + eventsPerBatch + 1).toVector
            .map(offsetWithEventIdEntries(_)._2)

          _ <- participant.prune(offsetToPruneUpTo)

          prunedEventsViaFlat <- Future.sequence(
            prunedEventIds.map(participant.flatTransactionByEventId(_, submitter).failed))

          _ <- Future.sequence(
            unprunedEventIds.map(participant.flatTransactionByEventId(_, submitter)))
        } yield {
          prunedEventsViaFlat.foreach(
            assertGrpcError(_, Status.Code.NOT_FOUND, "Transaction not found, or not visible."))
        }
    })

  test(
    "PRPruneRepeated",
    "Prune succeeds when called repeatedly",
    allocate(SingleParty),
    runConcurrently = false)(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        offsets <- populateLedgerAndGetOffsets(participant, submitter)
        offsetToPruneUpTo = offsets(lastItemToPruneIndex)
        offsetOfFirstSurvivingTransaction = offsets(lastItemToPruneIndex + 1)

        _ <- participant.prune(offsetToPruneUpTo)

        transactionsAfterPrune <- participant.transactionTrees(
          participant
            .getTransactionsRequest(parties = Seq(submitter))
            .update(_.begin := offsetToPruneUpTo))

        offsetAlreadyPruned = offsets(lastItemToPruneIndex / 2)

        _ <- participant.prune(offsetAlreadyPruned)

        transactionsAfterRedundantPrune <- participant.transactionTrees(
          participant
            .getTransactionsRequest(parties = Seq(submitter))
            .update(_.begin := offsetToPruneUpTo))

        offsetToPruneUpToInSecondRealPrune = offsets((lastItemToPruneIndex + 1) * 2 - 1)
        offsetOfFirstSurvivingTransactionInSecondPrune = offsets((lastItemToPruneIndex + 1) * 2)

        _ <- participant.prune(offsetToPruneUpToInSecondRealPrune)

        transactionsAfterSecondPrune <- participant.transactionTrees(
          participant
            .getTransactionsRequest(parties = Seq(submitter))
            .update(_.begin := offsetToPruneUpToInSecondRealPrune))

      } yield {
        assert(
          transactionsAfterPrune.size == offsets.size - (lastItemToPruneIndex + 1),
          s"transaction tree count after pruning does not match expected count"
        )
        assert(
          transactionsAfterPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
          s"transaction trees not pruned at expected offset"
        )
        assert(
          transactionsAfterRedundantPrune.size == offsets.size - (lastItemToPruneIndex + 1),
          s"transaction tree count after redundant pruning does not match expected count"
        )
        assert(
          transactionsAfterRedundantPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
          s"transaction trees not pruned at expected offset after redundant prune"
        )
        assert(
          transactionsAfterSecondPrune.size == offsets.size - 2 * (lastItemToPruneIndex + 1),
          s"transaction tree count after second pruning does not match expected count"
        )
        assert(
          transactionsAfterSecondPrune.head.offset == offsetOfFirstSurvivingTransactionInSecondPrune.getAbsolute,
          s"transaction trees not pruned at expected offset after second prune"
        )
      }
  })

  test(
    "PRPruneThenExercise",
    "Prune succeeds as observed by being able to exercise a contract created in pruned offset range",
    allocate(SingleParty),
    runConcurrently = false
  )(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        createdBeforePrune <- participant.create(submitter, Dummy(submitter))

        offsets <- populateLedgerAndGetOffsets(participant, submitter)
        offsetToPruneUpTo = offsets(lastItemToPruneIndex)

        _ <- participant.prune(offsetToPruneUpTo)

        _ <- participant.exercise(submitter, createdBeforePrune.exerciseDummyChoice1)
      } yield ()
  })

  test(
    "PRPruneQueryEmptyRangeOk",
    // This test is not terribly useful for conformance, but helps ensure that pruning does not interfere when run before
    // TransactionServiceIT "TXBeginToBegin" and "TXTreesBeginToBegin" tests.
    "Prune succeeds and does not prevent querying empty ranges even in pruned space",
    allocate(SingleParty),
    runConcurrently = false
  )(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        offsets <- populateLedgerAndGetOffsets(participant, submitter)
        offsetInPrunedRange = offsets(lastItemToPruneIndex / 2)
        offsetToPruneUpTo = offsets(lastItemToPruneIndex)

        _ <- participant.prune(offsetToPruneUpTo)

        emptyRangeAtBegin = participant
          .getTransactionsRequest(parties = Seq(submitter), begin = participant.begin)
          .update(_.end := participant.begin)

        emptyRangeInPrunedSpace = participant
          .getTransactionsRequest(parties = Seq(submitter), begin = offsetInPrunedRange)
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

  private def populateLedgerAndGetOffsets(participant: ParticipantTestContext, submitter: Party)(
      implicit ec: ExecutionContext): Future[Vector[LedgerOffset]] =
    populateLedger(participant, submitter)
      .map(_.map(tree => LedgerOffset.of(LedgerOffset.Value.Absolute(tree.offset))))

  private def populateLedgerAndGetOffsetsWithTransactionIds(
      participant: ParticipantTestContext,
      submitter: Party)(implicit ec: ExecutionContext): Future[Vector[(LedgerOffset, String)]] =
    populateLedger(participant, submitter)
      .map(_.map(tree =>
        (LedgerOffset.of(LedgerOffset.Value.Absolute(tree.offset)), tree.transactionId)))

  private def populateLedgerAndGetOffsetsWithEventIds(
      participant: ParticipantTestContext,
      submitter: Party)(implicit ec: ExecutionContext): Future[Vector[(LedgerOffset, String)]] =
    populateLedger(participant, submitter)
      .map(_.map(tree =>
        (LedgerOffset.of(LedgerOffset.Value.Absolute(tree.offset)), tree.eventsById.keys.head)))

  private def populateLedger(participant: ParticipantTestContext, submitter: Party)(
      implicit ec: ExecutionContext): Future[Vector[TransactionTree]] =
    for {
      endOffsetAtTestStart <- participant.currentEnd()
      _ <- Future
        .sequence(Vector.fill(batchesToPopulate) {
          for {
            dummy <- participant.create(submitter, Dummy(submitter))
            _ <- participant.exercise(submitter, dummy.exerciseDummyChoice1)
            _ <- participant.create(submitter, Dummy(submitter))
          } yield ()
        })
      trees <- participant.transactionTrees(
        participant.getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))
    } yield trees
}
