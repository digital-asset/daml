// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.regex.Pattern

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.event.Event.Event
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.model.Test.Dummy
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class ParticipantPruningIT extends LedgerTestSuite {

  private val batchesToPopulate = 14
  private val itemsToPrune = batchesToPopulate + 1

  test(
    "PRFailPruneByNoOffset",
    "Pruning a participant without specifying an offset should fail",
    allocate(NoParties))(implicit ec => {
    case Participants(Participant(participant)) =>
      for {
        failure <- participant.prune(None, 1).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "prune_up_to not specified")
      }
  })

  test(
    "PRFailPruneByBoundaryOffset",
    "Pruning a participant specifying a boundary offset should fail",
    allocate(NoParties))(implicit ec => {
    case Participants(Participant(participant)) =>
      for {
        cannotPruneBegin <- participant
          .prune(
            LedgerOffset.of(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)),
            1)
          .failed
        cannotPruneEnd <- participant
          .prune(
            LedgerOffset.of(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
            1)
          .failed
      } yield {
        assertGrpcError(
          cannotPruneBegin,
          Status.Code.INVALID_ARGUMENT,
          "prune_up_to needs to be absolute and not a boundary LEDGER_BEGIN")
        assertGrpcError(
          cannotPruneEnd,
          Status.Code.INVALID_ARGUMENT,
          "prune_up_to needs to be absolute and not a boundary LEDGER_END")
      }
  })

  test(
    "PRFailPruneByNonHexOffset",
    "Pruning a participant specifying a non-hexadecimal offset should fail",
    allocate(NoParties))(implicit ec => {
    case Participants(Participant(participant)) =>
      for {
        cannotPruneNonHexOffset <- participant
          .prune(LedgerOffset.of(LedgerOffset.Value.Absolute("cofefe")), 1)
          .failed
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
    allocate(NoParties))(implicit ec => {
    case Participants(Participant(participant)) =>
      for {
        actualEndExclusive <- participant.currentEnd()
        cannotPruneOffsetBeyondEnd <- participant
          .prune(actualEndExclusive, 1)
          .failed
      } yield {
        assertGrpcError(
          cannotPruneOffsetBeyondEnd,
          Status.Code.INVALID_ARGUMENT,
          "prune_up_to needs to be before ledger end")
      }
  })

  test("PRPruneTxTrees", "Prune succeeds as observed by transaction trees", allocate(SingleParty))(
    implicit ec => {
      case Participants(Participant(participant, submitter)) =>
        for {
          endOffsetAtTestStart <- participant.currentEnd()

          size <- populateLedger(participant, submitter, batchesToPopulate)

          txBeforePrune <- participant.transactionTrees(
            participant
              .getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))

          offsetToPruneUpTo = offsetAt[TransactionTree](txBeforePrune, itemsToPrune, _.offset)

          offsetOfFirstSurvivingTransaction = offsetAt[TransactionTree](
            txBeforePrune,
            itemsToPrune + 1,
            _.offset)

          _ <- participant.prune(offsetToPruneUpTo)

          txAfterPrune <- participant.transactionTrees(
            participant
              .getTransactionsRequest(Seq(submitter))
              .update(_.begin := offsetToPruneUpTo))

          cannotReadAnymore <- participant
            .transactionTrees(
              participant
                .getTransactionsRequest(Seq(submitter))
                .update(
                  _.begin := offsetAt[TransactionTree](
                    txBeforePrune,
                    itemsToPrune - 1, // This offset is the largest exclusive offset we can no longer read from after
                    _.offset))
            )
            .failed
        } yield {
          assert(txBeforePrune.size == size)
          assert(
            txAfterPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
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

  test("PRPruneTxFlat", "Prune succeeds as observed by flat transactions", allocate(SingleParty))(
    implicit ec => {
      case Participants(Participant(participant, submitter)) =>
        for {
          endOffsetAtTestStart <- participant.currentEnd()

          size <- populateLedger(participant, submitter, batchesToPopulate)

          txBeforePrune <- participant.flatTransactions(
            participant
              .getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))

          offsetToPruneUpTo = offsetAt[Transaction](txBeforePrune, itemsToPrune, _.offset)

          offsetOfFirstSurvivingTransaction = offsetAt[Transaction](
            txBeforePrune,
            itemsToPrune + 1,
            _.offset)

          _ <- participant.prune(offsetToPruneUpTo)

          txAfterPrune <- participant.flatTransactions(
            participant
              .getTransactionsRequest(Seq(submitter))
              .update(_.begin := offsetToPruneUpTo))

          cannotReadAnymore <- participant
            .flatTransactions(
              participant
                .getTransactionsRequest(Seq(submitter))
                .update(
                  _.begin := offsetAt[Transaction](
                    txBeforePrune,
                    itemsToPrune - 1, // This offset is the largest exclusive offset we can no longer read from after
                    _.offset))
            )
            .failed
        } yield {
          assert(txBeforePrune.size == size)
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
    allocate(SingleParty))(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        endOffsetAtTestStart <- participant.currentEnd()

        _ <- populateLedger(participant, submitter, batchesToPopulate)

        firstCheckpointsBeforePrune <- participant
          .checkpoints(1, participant.completionStreamRequest(endOffsetAtTestStart)(submitter))
          .map(_.head)

        transactionsLookup <- participant.transactionTrees(
          participant
            .getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))

        offsetToPruneUpTo = offsetAt[TransactionTree](transactionsLookup, itemsToPrune, _.offset)

        offsetOfFirstSurvivingCheckpoint = offsetAt[TransactionTree](
          transactionsLookup,
          itemsToPrune + 1,
          _.offset)

        _ <- participant.prune(offsetToPruneUpTo)

        firstCheckpointsAfterPrune <- participant
          .checkpoints(1, participant.completionStreamRequest(offsetToPruneUpTo)(submitter))
          .map(_.head)

        cannotReadAnymore <- participant
          .checkpoints(
            1,
            participant.completionStreamRequest(
              offsetAt[TransactionTree](
                transactionsLookup,
                itemsToPrune - 1, // This offset is the largest exclusive offset we can no longer read from after
                _.offset))(submitter)
          )
          .failed
      } yield {
        assert(firstCheckpointsBeforePrune.offset.exists(o =>
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

  test("PRPruneACS", "Prune succeeds by not affecting active contracts", allocate(SingleParty))(
    implicit ec => {
      case Participants(Participant(participant, submitter)) =>
        for {
          endOffsetAtTestStart <- participant.currentEnd()

          _ <- populateLedger(participant, submitter, batchesToPopulate)

          createdBefore <- participant.activeContracts(submitter)

          transactionsLookup <- participant.transactionTrees(
            participant
              .getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))

          offsetToPruneUpTo = offsetAt[TransactionTree](transactionsLookup, itemsToPrune, _.offset)

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
    allocate(SingleParty))(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        endOffsetAtTestStart <- participant.currentEnd()

        size <- populateLedger(participant, submitter, batchesToPopulate)

        transactionsPerBatch = size / batchesToPopulate

        transactionsLookup <- participant.transactionTrees(
          participant
            .getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))

        offsetToPruneUpTo = offsetAt[TransactionTree](transactionsLookup, itemsToPrune, _.offset)

        _ <- participant.prune(offsetToPruneUpTo)

        prunedTransactionIds = Range(itemsToPrune - transactionsPerBatch, itemsToPrune).toVector
          .map(transactionsLookup(_).transactionId)
        unprunedTransactionIds = Range(itemsToPrune, itemsToPrune + transactionsPerBatch)
          .map(transactionsLookup(_).transactionId)
          .toVector

        prunedTransactionTrees <- Future.sequence(
          prunedTransactionIds.map(participant.transactionTreeById(_, submitter).failed))

        unprunedTransactionTrees <- Future.sequence(
          unprunedTransactionIds.map(participant.transactionTreeById(_, submitter)))
      } yield {
        assert(
          prunedTransactionTrees.size == transactionsPerBatch,
          "None of the pruned transaction trees should be visible")
        assert(
          unprunedTransactionTrees.size == transactionsPerBatch,
          "All unpruned transaction trees should be visible")
        prunedTransactionTrees.foreach(
          assertGrpcError(_, Status.Code.NOT_FOUND, "Transaction not found, or not visible."))
      }
  })

  test(
    "PRPruneFlatByTxId",
    "Prune succeeds as observed by individual flat transaction lookups",
    allocate(SingleParty))(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        endOffsetAtTestStart <- participant.currentEnd()

        size <- populateLedger(participant, submitter, batchesToPopulate)

        transactionsPerBatch = size / batchesToPopulate

        transactionsLookup <- participant.transactionTrees(
          participant
            .getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))

        offsetToPruneUpTo = offsetAt[TransactionTree](transactionsLookup, itemsToPrune, _.offset)

        _ <- participant.prune(offsetToPruneUpTo)

        prunedTransactionIds = Range(itemsToPrune - transactionsPerBatch, itemsToPrune).toVector
          .map(transactionsLookup(_).transactionId)
        unprunedTransactionIds = Range(itemsToPrune, itemsToPrune + transactionsPerBatch)
          .map(transactionsLookup(_).transactionId)
          .toVector

        prunedFlatTransactions <- Future.sequence(
          prunedTransactionIds.map(participant.flatTransactionById(_, submitter).failed))

        unprunedFlatTransactions <- Future.sequence(
          unprunedTransactionIds.map(participant.flatTransactionById(_, submitter)))
      } yield {
        assert(
          prunedFlatTransactions.size == transactionsPerBatch,
          "None of the pruned flat transactions should be visible")
        assert(
          unprunedFlatTransactions.size == transactionsPerBatch,
          "All unpruned flat transactions should be visible")
        prunedFlatTransactions.foreach(
          assertGrpcError(_, Status.Code.NOT_FOUND, "Transaction not found, or not visible."))
      }
  })

  test(
    "PRPruneTreeByEventId",
    "Prune succeeds as observed by individual event lookups via transaction tree",
    allocate(SingleParty))(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        endOffsetAtTestStart <- participant.currentEnd()

        size <- populateLedger(participant, submitter, batchesToPopulate)

        transactionsPerBatch = size / batchesToPopulate

        transactionsLookup <- participant.transactionTrees(
          participant
            .getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))

        eventIdsLookup <- participant
          .flatTransactions(
            participant
              .getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))
          .map(_.map(_.events.head.event match {
            case ec: Event.Created => ec.value.eventId
            case ea: Event.Archived => ea.value.eventId
            case Event.Empty => fail("should not find empty event")
          }))

        offsetToPruneUpTo = offsetAt[TransactionTree](transactionsLookup, itemsToPrune, _.offset)

        _ <- participant.prune(offsetToPruneUpTo)

        prunedEventIds = Range(itemsToPrune - transactionsPerBatch, itemsToPrune).toVector
          .map(eventIdsLookup)
        unprunedEventIds = Range(itemsToPrune, itemsToPrune + transactionsPerBatch).toVector
          .map(eventIdsLookup)

        prunedEventsViaTree <- Future.sequence(
          prunedEventIds.map(participant.transactionTreeByEventId(_, submitter).failed))

        unprunedEventsViaTree <- Future.sequence(
          unprunedEventIds.map(participant.transactionTreeByEventId(_, submitter)))
      } yield {
        assert(
          prunedEventsViaTree.size == transactionsPerBatch,
          "None of the pruned events via transaction trees should be visible")
        assert(
          unprunedEventsViaTree.size == transactionsPerBatch,
          "All unpruned events via transaction trees should be visible")
        prunedEventsViaTree.foreach(
          assertGrpcError(_, Status.Code.NOT_FOUND, "Transaction not found, or not visible."))
      }
  })

  test(
    "PRPruneFlatByEventId",
    "Prune succeeds as observed by individual event lookups via flat transaction",
    allocate(SingleParty))(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        endOffsetAtTestStart <- participant.currentEnd()

        size <- populateLedger(participant, submitter, batchesToPopulate)

        transactionsPerBatch = size / batchesToPopulate

        transactionsLookup <- participant.transactionTrees(
          participant
            .getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))

        eventIdsLookup <- participant
          .flatTransactions(
            participant
              .getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))
          .map(_.map(_.events.head.event match {
            case ec: Event.Created => ec.value.eventId
            case ea: Event.Archived => ea.value.eventId
            case Event.Empty => fail("should not find empty event")
          }))

        offsetToPruneUpTo = offsetAt[TransactionTree](transactionsLookup, itemsToPrune, _.offset)

        _ <- participant.prune(offsetToPruneUpTo)

        prunedEventIds = Range(itemsToPrune - transactionsPerBatch, itemsToPrune).toVector
          .map(eventIdsLookup)
        unprunedEventIds = Range(itemsToPrune, itemsToPrune + transactionsPerBatch).toVector
          .map(eventIdsLookup)

        prunedEventsViaFlat <- Future.sequence(
          prunedEventIds.map(participant.flatTransactionByEventId(_, submitter).failed))

        unprunedEventsViaFlat <- Future.sequence(
          unprunedEventIds.map(participant.flatTransactionByEventId(_, submitter)))
      } yield {
        assert(
          prunedEventsViaFlat.size == transactionsPerBatch,
          "None of the pruned events via flat transactions should be visible")
        assert(
          unprunedEventsViaFlat.size == transactionsPerBatch,
          "All unpruned events via flat transactions should be visible")
        prunedEventsViaFlat.foreach(
          assertGrpcError(_, Status.Code.NOT_FOUND, "Transaction not found, or not visible."))
      }
  })

  test("PRPruneRepeated", "Prune succeeds when called repeatedly", allocate(SingleParty))(
    implicit ec => {
      case Participants(Participant(participant, submitter)) =>
        for {
          endOffsetAtTestStart <- participant.currentEnd()

          size <- populateLedger(participant, submitter, batchesToPopulate)

          txBeforePrune <- participant.transactionTrees(
            participant
              .getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))

          offsetToPruneUpTo = offsetAt[TransactionTree](txBeforePrune, itemsToPrune, _.offset)

          offsetOfFirstSurvivingTransaction = offsetAt[TransactionTree](
            txBeforePrune,
            itemsToPrune + 1,
            _.offset)

          _ <- participant.prune(offsetToPruneUpTo)

          txAfterPrune <- participant.transactionTrees(
            participant
              .getTransactionsRequest(Seq(submitter))
              .update(_.begin := offsetToPruneUpTo))

          offsetAlreadyPruned = offsetAt[TransactionTree](txBeforePrune, itemsToPrune / 2, _.offset)

          _ <- participant.prune(offsetAlreadyPruned)

          txAfterRedundantPrune <- participant.transactionTrees(
            participant
              .getTransactionsRequest(Seq(submitter))
              .update(_.begin := offsetToPruneUpTo))

          offsetToPruneUpToInSecondRealPrune = offsetAt[TransactionTree](
            txBeforePrune,
            itemsToPrune * 2,
            _.offset)

          offsetOfFirstSurvivingTransactionInSecondPrune = offsetAt[TransactionTree](
            txBeforePrune,
            itemsToPrune * 2 + 1,
            _.offset)

          _ <- participant.prune(offsetToPruneUpTo)

          txAfterSecondPrune <- participant.transactionTrees(
            participant
              .getTransactionsRequest(Seq(submitter))
              .update(_.begin := offsetToPruneUpToInSecondRealPrune))

        } yield {
          assert(txBeforePrune.size == size)
          assert(
            txAfterPrune.size == size - itemsToPrune,
            s"transaction tree count after pruning does not match expected count"
          )
          assert(
            txAfterPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
            s"transaction trees not pruned at expected offset"
          )
          assert(
            txAfterRedundantPrune.size == size - itemsToPrune,
            s"transaction tree count after redundant pruning does not match expected count"
          )
          assert(
            txAfterRedundantPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
            s"transaction trees not pruned at expected offset after redundant prune"
          )
          assert(
            txAfterSecondPrune.size == size - 2 * itemsToPrune,
            s"transaction tree count after second pruning does not match expected count"
          )
          assert(
            txAfterSecondPrune.head.offset == offsetOfFirstSurvivingTransactionInSecondPrune.getAbsolute,
            s"transaction trees not pruned at expected offset after second prune"
          )
        }
    })

  test(
    "PRPruneThenExercise",
    "Prune succeeds as observed by being able to exercise a contract created in pruned offset range",
    allocate(SingleParty))(implicit ec => {
    case Participants(Participant(participant, submitter)) =>
      for {
        createdBeforePrune <- participant.create(submitter, Dummy(submitter))

        endOffsetAtTestStart <- participant.currentEnd()

        _ <- populateLedger(participant, submitter, batchesToPopulate)

        txBeforePrune <- participant.flatTransactions(
          participant
            .getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart))

        offsetToPruneUpTo = offsetAt[Transaction](txBeforePrune, itemsToPrune, _.offset)

        _ <- participant.prune(offsetToPruneUpTo)

        _ <- participant.exercise(submitter, createdBeforePrune.exerciseDummyChoice1)
      } yield ()
  })

  private def populateLedger(participant: ParticipantTestContext, submitter: Party, n: Int)(
      implicit ec: ExecutionContext): Future[Int] =
    Future
      .sequence(
        Vector.fill(n) {
          for {
            dummy <- participant.create(submitter, Dummy(submitter))
            _ <- participant.exercise(submitter, dummy.exerciseDummyChoice1)
            _ <- participant.create(submitter, Dummy(submitter))
          } yield ()
        }
      )
      .map(_ => n * 3) // three events per batch

  private def offsetAt[A](v: Vector[A], n: Int, getOffset: A => String): LedgerOffset =
    LedgerOffset.of(LedgerOffset.Value.Absolute(getOffset(v(n - 1))))
}
