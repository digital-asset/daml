// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.util.regex.Pattern
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.testtool.infrastructure.Allocation.{Participant, _}
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.{FutureAssertions, LedgerTestSuite}
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.TransactionTree
import com.daml.ledger.api.v1.transaction_service.{
  GetEventsByContractIdRequest,
  GetEventsByContractKeyRequest,
}
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.model.Test.{Dummy, TextKey}
import com.daml.ledger.test.semantic.DivulgenceTests._
import com.daml.logging.LoggingContext
import scalaz.Tag
import scalaz.syntax.tag.ToTagOps

import scala.collection.immutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class ParticipantPruningIT extends LedgerTestSuite {

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
      createdBeforePrune <- participant.create(submitter, Dummy(submitter))

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
      contract <- participant.create(alice, Contract(alice))

      // Retroactively divulge Alice's contract to bob
      _ <- participant.exercise(alice, divulgence.exerciseDivulge(contract))

      // Bob can see the divulged contract
      _ <- participant.exerciseAndGetContract[Dummy](
        bob,
        divulgence.exerciseCanFetch(contract),
      )

      _ <- pruneAtCurrentOffset(
        participant,
        bob,
        pruneAllDivulgedContracts = false,
      )

      // Bob can still see the divulged contract
      _ <- participant.exerciseAndGetContract[Dummy](
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
        .exerciseAndGetContract[Dummy](
          bob,
          divulgence.exerciseCanFetch(contract),
        )
        .mustFailWith("Bob cannot access a divulged contract which was already archived") {
          exception =>
            val errorMessage = exception.getMessage
            errorMessage.contains(
              "Contract could not be found with id"
            ) && errorMessage.contains(contract.toString)
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
      contract <- alpha.create(alice, Contract(alice))

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

      divulgeNotDiscloseTemplate <- alpha.create(alice, DivulgeNotDiscloseTemplate(alice, bob))

      // Alice creates contract in a context not visible to Bob and follows with a divulgence to Bob in the same transaction
      contract <- alpha.exerciseAndGetContract[Contract](
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
      contract <- alpha.exerciseAndGetContract[Contract](
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
  )(implicit ec => { case Participants(Participant(participant, party)) =>
    def getEvents(dummyCid: Primitive.ContractId[Dummy]): Future[Int] = {
      val request = GetEventsByContractIdRequest(dummyCid.unwrap, immutable.Seq(party.unwrap))
      participant.getEventsByContractId(request).map(_.events.size)
    }

    for {
      dummyCid <- participant.create(party, Dummy(party))
      end1 <- pruneToCurrentEnd(participant, party)
      events1 <- getEvents(dummyCid)
      exerciseCmd = participant.submitAndWaitRequest(party, dummyCid.exerciseDummyChoice1().command)
      _ <- participant.submitAndWaitForTransactionTree(exerciseCmd)
      events2 <- getEvents(dummyCid)
      _ <- participant.prune(end1)
      events3 <- getEvents(dummyCid)
      _ <- pruneToCurrentEnd(participant, party)
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
  )(implicit ec => { case Participants(Participant(participant, party)) =>
    val exercisedKey = "pruning test key"
    val key = makeTextKeyKey(party, exercisedKey)

    def getEvents: Future[(Int, String)] = participant
      .getEventsByContractKey(
        GetEventsByContractKeyRequest(
          contractKey = Some(key),
          templateId = Some(TextKey.id.unwrap),
          requestingParties = Tag.unsubst(immutable.Seq(party)),
          maxEvents = 10,
          beginExclusive = None,
          endInclusive = None,
        )
      )
      .map(r =>
        (
          r.events.size,
          assertDefined(
            r.prunedOffset.flatMap(_.value.absolute),
            "Expected populated absolute offset",
          ),
        )
      )

    for {
      textKeyCid1 <- participant.create(party, TextKey(party, exercisedKey, Nil))
      end1 <- pruneToCurrentEnd(participant, party).map(_.getAbsolute)
      (events1, offset1) <- getEvents
      exerciseCmd = participant.submitAndWaitRequest(
        party,
        textKeyCid1.exerciseTextKeyChoice().command,
      )
      _ <- participant.submitAndWaitForTransactionTree(exerciseCmd)
      (events2, _) <- getEvents
      end3 <- pruneToCurrentEnd(participant, party).map(_.getAbsolute)
      (events3, offset3) <- getEvents
    } yield {
      assertEquals("Expected single create event after prune", events1, 1)
      assertEquals("Expected create and consume event before prune", events2, 2)
      assertEquals("Expected no events following prune", events3, 0)

      assert(
        offset1 >= end1,
        s"Expected reported prune end ($offset1) at #1 to be equal or higher than last known prune ($end1)",
      )
      assert(
        offset3 >= end3,
        s"Expected reported prune end ($offset3) at #3 to be equal or higher than last known prune ($end3)",
      )
    }
  })

  private def createDivulgence(
      alice: Party,
      bob: Party,
      alpha: ParticipantTestContext,
      beta: ParticipantTestContext,
  )(implicit ec: ExecutionContext) =
    for {
      divulgenceHelper <- alpha.create(alice, DivulgenceProposal(alice, bob))
      _ <- synchronize(alpha, beta)
      divulgence <- beta.exerciseAndGetContract[Divulgence](bob, divulgenceHelper.exerciseAccept())
    } yield divulgence

  private def divulgencePruneAndCheck(
      alice: Party,
      bob: Party,
      alpha: ParticipantTestContext,
      beta: ParticipantTestContext,
      contract: Primitive.ContractId[Contract],
      divulgence: Primitive.ContractId[Divulgence],
  )(implicit ec: ExecutionContext) =
    for {
      _ <- synchronize(alpha, beta)

      // Check that Bob can fetch the contract
      _ <- beta.exerciseAndGetContract[Dummy](
        bob,
        divulgence.exerciseCanFetch(contract),
      )

      offsetAfterDivulgence_1 <- beta.currentEnd()

      // Alice re-divulges the contract to Bob
      _ <- alpha.exerciseAndGetContract[Contract](
        alice,
        divulgence.exerciseDivulge(contract),
      )

      _ <- synchronize(alpha, beta)

      // Check that Bob can fetch the contract
      _ <- beta.exerciseAndGetContract[Dummy](
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
      _ <- beta.exerciseAndGetContract[Dummy](
        bob,
        divulgence.exerciseCanFetch(contract),
      )

      // Populate "other" participant too to advance canton's safe pruning offset
      _ <- populateLedgerAndGetOffsets(alpha, alice)

      _ <- pruneAtCurrentOffset(beta, bob, pruneAllDivulgedContracts = true)

      _ <- beta
        .exerciseAndGetContract[Dummy](
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
            dummy <- participant.create(submitter, Dummy(submitter))
            _ <- participant.exercise(submitter, dummy.exerciseDummyChoice1())
            _ <- participant.create(submitter, Dummy(submitter))
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
      _ <- participant.create(localParty, Dummy(localParty))

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
  private def makeTextKeyKey(party: ApiTypes.Party, keyText: String) = {
    Value(
      Value.Sum.Record(
        Record(fields =
          Vector(
            RecordField(value = Some(Value(Value.Sum.Party(party.unwrap)))),
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
        _ <- ledger.submitAndWait(ledger.submitAndWaitRequest(party, Dummy(party).create.command))
        _ <- ledger.prune(pruneUpTo = pruneUpTo, attempts = 1)
      } yield ()
    }

}
