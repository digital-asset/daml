// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import java.util.regex.Pattern

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.TransactionTree
import com.daml.ledger.client.binding
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.model.Test.Dummy
import com.daml.ledger.test.semantic.DivulgenceTests._
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class ParticipantPruningIT extends LedgerTestSuite {

  private val batchesToPopulate = 74
  private val lastItemToPruneIndex = batchesToPopulate

  test(
    "PRFailPruneByNoOffset",
    "Pruning a participant without specifying an offset should fail",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(participant)) =>
    for {
      failure <- participant
        // TODO Divulgence pruning: Change to `true` once all divulgence pruning is implemented
        .prune("", attempts = 1, pruneAllDivulgedContracts = false)
        .mustFail("pruning without specifying an offset")
    } yield {
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "prune_up_to not specified")
    }
  })

  test(
    "PRFailPruneByNonHexOffset",
    "Pruning a participant specifying a non-hexadecimal offset should fail",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(participant)) =>
    for {
      cannotPruneNonHexOffset <- participant
        // TODO Divulgence pruning: Change to `true` once all divulgence pruning is implemented
        .prune("covfefe", attempts = 1, pruneAllDivulgedContracts = false)
        .mustFail("pruning, specifiying a non-hexadecimal offset")
    } yield {
      assertGrpcError(
        cannotPruneNonHexOffset,
        Status.Code.INVALID_ARGUMENT,
        "prune_up_to needs to be a hexadecimal string and not",
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
        .mustFail("pruning, specifiying an offset after the ledger end")
    } yield {
      assertGrpcError(
        cannotPruneOffsetBeyondEnd,
        Status.Code.INVALID_ARGUMENT,
        "prune_up_to needs to be before ledger end",
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
          .getTransactionsRequest(parties = Seq(submitter))
          .update(_.begin := offsetToPruneUpTo)
      )

      cannotReadAnymore <- participant
        .transactionTrees(
          participant
            .getTransactionsRequest(parties = Seq(submitter))
            .update(_.begin := offsetOfSecondToLastPrunedTransaction)
        )
        .mustFail("attempting to read transactions before the pruning cut-off")
    } yield {
      assert(
        transactionsAfterPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
        s"transaction trees not pruned at expected offset",
      )
      assertGrpcError(
        cannotReadAnymore,
        Status.Code.NOT_FOUND,
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
          .getTransactionsRequest(parties = Seq(submitter))
          .update(_.begin := offsetToPruneUpTo)
      )

      cannotReadAnymore <- participant
        .flatTransactions(
          participant
            .getTransactionsRequest(parties = Seq(submitter))
            .update(_.begin := offsetOfSecondToLastPrunedTransaction)
        )
        .mustFail("attempting to read transactions before the pruning cut-off")
    } yield {
      assert(
        txAfterPrune.head.offset == offsetOfFirstSurvivingTransaction.getAbsolute,
        s"flat transactions not pruned at expected offset",
      )
      assertGrpcError(
        cannotReadAnymore,
        Status.Code.NOT_FOUND,
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
      assertGrpcError(
        cannotReadAnymore,
        Status.Code.NOT_FOUND,
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
        assertGrpcError(_, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
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
        assertGrpcError(_, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
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
        assertGrpcError(_, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
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
        assertGrpcError(_, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
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
          .getTransactionsRequest(parties = Seq(submitter))
          .update(_.begin := offsetToPruneUpTo)
      )

      offsetAlreadyPruned = offsets(lastItemToPruneIndex / 2)

      _ <- participant.prune(offsetAlreadyPruned)

      transactionsAfterRedundantPrune <- participant.transactionTrees(
        participant
          .getTransactionsRequest(parties = Seq(submitter))
          .update(_.begin := offsetToPruneUpTo)
      )

      offsetToPruneUpToInSecondRealPrune = offsets((lastItemToPruneIndex + 1) * 2 - 1)
      offsetOfFirstSurvivingTransactionInSecondPrune = offsets((lastItemToPruneIndex + 1) * 2)

      _ <- participant.prune(offsetToPruneUpToInSecondRealPrune)

      transactionsAfterSecondPrune <- participant.transactionTrees(
        participant
          .getTransactionsRequest(parties = Seq(submitter))
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
        transactionsAfterSecondPrune.size == offsets.size - 2 * (lastItemToPruneIndex + 1),
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

      _ <- participant.exercise(submitter, createdBeforePrune.exerciseDummyChoice1)
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

  test(
    "PRRetroactiveDivulgences",
    "Divulgence pruning succeeds",
    allocate(SingleParty, SingleParty),
    runConcurrently = false, // pruning call may interact with other tests
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      divulgence <- createDivulgence(alice, bob, alpha, beta)
      contract <- alpha.create(alice, Contract(alice))

      // Retroactively divulge Alice's contract to bob
      _ <- alpha.exercise(
        alice,
        divulgence.exerciseDivulge(_, contract),
      )

      _ <- divulgencePruneAndCheck(alice, bob, alpha, beta, contract, divulgence)
    } yield ()
  })

  test(
    "PRLocalAndNonLocalRetroactiveDivulgences",
    "Divuglence pruning succeeds if first divulgence is not a disclosure but happens in the same transaction as the create",
    allocate(SingleParty, SingleParty),
    runConcurrently = false, // pruning call may interact with other tests
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      divulgence <- createDivulgence(alice, bob, alpha, beta)

      divulgeNotDiscloseTemplate <- alpha.create(alice, DivulgeNotDiscloseTemplate(alice, bob))

      // Alice creates contract in a context not visible to Bob and follows with a divulgence to Bob in the same transaction
      contract <- alpha.exerciseAndGetContract[Contract](
        alice,
        divulgeNotDiscloseTemplate.exerciseDivulgeNoDisclose(_, divulgence),
      )

      _ <- divulgencePruneAndCheck(alice, bob, alpha, beta, contract, divulgence)
    } yield ()
  })

  test(
    "PRDisclosureAndRetroactiveDivulgence",
    "Disclosure pruning succeeds",
    allocate(SingleParty, SingleParty),
    runConcurrently = false, // pruning call may interact with other tests
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      divulgence <- createDivulgence(alice, bob, alpha, beta)
      // Alice's contract creation is disclosed to Bob
      contract <- alpha.exerciseAndGetContract[Contract](
        alice,
        divulgence.exerciseCreateAndDisclose,
      )
      _ <- divulgencePruneAndCheck(alice, bob, alpha, beta, contract, divulgence)
    } yield ()
  })

  private def createDivulgence(
      alice: Party,
      bob: Party,
      alpha: ParticipantTestContext,
      beta: ParticipantTestContext,
  )(implicit ec: ExecutionContext) =
    for {
      divulgenceHelper <- alpha.create(alice, DivulgenceProposal(alice, bob))
      divulgence <- beta.exerciseAndGetContract[Divulgence](bob, divulgenceHelper.exerciseAccept)
    } yield divulgence

  private def divulgencePruneAndCheck(
      alice: Party,
      bob: Party,
      alpha: ParticipantTestContext,
      beta: ParticipantTestContext,
      contract: Primitive.ContractId[Contract],
      divulgence: binding.Primitive.ContractId[Divulgence],
  )(implicit ec: ExecutionContext) =
    for {
      // Check that Bob can fetch the contract
      _ <- beta.exerciseAndGetContract[Dummy](
        bob,
        divulgence.exerciseCanFetch(_, contract),
      )

      offsetAfter_divulgence_1 <- beta.currentEnd()

      // Alice re-divulges the contract to Bob
      _ <- alpha.exerciseAndGetContract[Contract](
        alice,
        divulgence.exerciseDivulge(_, contract),
      )

      // Check that Bob can fetch the contract
      _ <- beta.exerciseAndGetContract[Dummy](
        bob,
        divulgence.exerciseCanFetch(_, contract),
      )
      offsetAfter_divulgence_2 <- beta.currentEnd()

      _ <- beta.prune(offsetAfter_divulgence_1)
      // Check that Bob can still fetch the contract after pruning the first transaction
      _ <- beta.exerciseAndGetContract[Dummy](
        bob,
        divulgence.exerciseCanFetch(_, contract),
      )

      _ <- beta.prune(offsetAfter_divulgence_2)
      // TODO divulgence pruning: Remove - The following assertion should fail once full divulgence pruning
      //                          is implemented in the participant
      _ <- beta
        .exerciseAndGetContract[Dummy](
          bob,
          divulgence.exerciseCanFetch(_, contract),
        )

      // TODO divulgence pruning: Un-comment the assertion below to make tests pass once
      //                          full divulgence pruning is implemented in the participant
      //
      // _ <- beta
      //   .exerciseAndGetContract[Dummy](
      //     bob,
      //     divulgence.exerciseCanFetch(_, contract),
      //   )
      //   .mustFail("Bob cannot access the divulged contract after the second pruning")
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
            _ <- participant.exercise(submitter, dummy.exerciseDummyChoice1)
            _ <- participant.create(submitter, Dummy(submitter))
          } yield ()
        })
      trees <- participant.transactionTrees(
        participant.getTransactionsRequest(parties = Seq(submitter), begin = endOffsetAtTestStart)
      )
    } yield trees
}
