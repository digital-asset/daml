// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse.CompletionResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.offset_checkpoint.OffsetCheckpoint
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.ledger.test.java.model.test.Dummy
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.time.NonNegativeFiniteDuration

import scala.concurrent.Future

class CheckpointInTailingStreamsIT extends LedgerTestSuite {
  import CompanionImplicits.*
  import com.daml.ledger.api.testtool.suites.v2_2.CheckpointInTailingStreamsIT.*

  test(
    "TXServeTailingStreamCheckpointAtTheEnd",
    "Tailing transaction streams should contain a checkpoint message",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 10
    val transactionsToRead = 10
    for {
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      // sleep for 3 * maxOffsetCheckpointEmissionDelay to ensure that the offset checkpoint cache is updated
      _ <- Future(
        Thread.sleep(3 * ledger.maxOffsetCheckpointEmissionDelay.duration.toMillis)
      )
      updates <- ledger.updates(
        within,
        ledger
          .getTransactionsRequestWithEnd(
            transactionFormat = ledger.transactionFormat(
              parties = Some(Seq(party)),
              transactionShape = AcsDelta,
            ),
            end = None,
          ),
      )
      txs: Vector[Transaction] = updates.flatMap(_.transaction)
      checkpoints: Vector[OffsetCheckpoint] = updates.flatMap(_.offsetCheckpoint)
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        updates.size > transactionsToRead,
        s"More than $transactionsToRead updates should have been received but ${updates.size} were instead",
      )
      assert(
        txs.size == transactionsToRead,
        s"$transactionsToRead transactions should have been received but ${txs.size} were instead",
      )
      assert(
        checkpoints.nonEmpty,
        s"At least one checkpoint should have been received but none were instead",
      )
      assertUpdatesInOrder(updates)
    }
  })

  test(
    "TXServeTailingStreamCheckpointTimeout",
    "Tailing transaction streams should contain a checkpoint message when idle",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party, party2))) =>
    val transactionsToSubmit = 10
    for {
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      updates <- ledger.updates(
        ledger.maxOffsetCheckpointEmissionDelay * NonNegativeInt.tryCreate(3),
        ledger
          .getTransactionsRequestWithEnd(
            transactionFormat = ledger.transactionFormat(parties = Some(Seq(party2))),
            end = None,
          ),
      )
      txs: Vector[Transaction] = updates.flatMap(_.transaction)
      checkpoints: Vector[OffsetCheckpoint] = updates.flatMap(_.offsetCheckpoint)
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        updates.nonEmpty,
        s"At least one update (checkpoint) should have been received but none were instead",
      )
      assert(
        txs.isEmpty,
        s"No transactions should have been received but ${txs.size} were instead",
      )
      assert(
        checkpoints.nonEmpty,
        s"At least one checkpoint should have been received but none were instead",
      )
      assertUpdatesInOrder(updates)
    }
  })

  test(
    "TXServeTailingStreamCheckpointEmpty",
    "Tailing transaction streams should contain a checkpoint message if there are any offset updates (even if they are for other parties)",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party, party2))) =>
    val transactionsToSubmit = 10
    for {
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      // sleep for 3 * maxOffsetCheckpointEmissionDelay to ensure that the offset checkpoint cache is updated
      _ <- Future(Thread.sleep(3 * ledger.maxOffsetCheckpointEmissionDelay.duration.toMillis))
      // fetching updates for party2 should return 0 txs
      updates <- ledger.updates(
        within,
        ledger
          .getTransactionsRequestWithEnd(
            transactionFormat = ledger.transactionFormat(Some(Seq(party2))),
            end = None,
          ),
      )
      txs: Vector[Transaction] = updates.flatMap(_.transaction)
      checkpoints: Vector[OffsetCheckpoint] = updates.flatMap(_.offsetCheckpoint)
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        updates.nonEmpty,
        s"At least one update should have been received but none were instead",
      )
      assert(
        txs.isEmpty,
        s"No transactions should have been received but ${txs.size} were instead",
      )
      assert(
        checkpoints.nonEmpty,
        s"At least one checkpoint should have been received but none were instead",
      )
      assertUpdatesInOrder(updates)
    }
  })

  test(
    "CompletionsStreamCheckpointAtTheEnd",
    "Command completions streams should contain a checkpoint message",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 10
    val transactionsToRead = 10
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      // sleep for 3 * maxOffsetCheckpointEmissionDelay to ensure that the offset checkpoint cache is updated
      _ <- Future(Thread.sleep(3 * ledger.maxOffsetCheckpointEmissionDelay.duration.toMillis))
      responses <- ledger.completions(
        within,
        ledger
          .completionStreamRequest(endOffsetAtTestStart)(party),
      )
      completions: Vector[Completion] = responses.flatMap(_.completion)
      checkpoints: Vector[OffsetCheckpoint] = responses.flatMap(_.offsetCheckpoint)
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        responses.size > transactionsToRead,
        s"More than ${transactionsToRead + 1} responses should have been received but ${responses.size} were instead",
      )
      assert(
        completions.size == transactionsToRead,
        s"$transactionsToRead completions should have been received but ${completions.size} were instead",
      )
      assert(
        checkpoints.nonEmpty,
        s"At least one checkpoint should have been received but none were instead",
      )
      assertCompletionsInOrder(responses)
    }
  })

  test(
    "CompletionsStreamCheckpointTimeout",
    "Command completions streams should contain a checkpoint message when idle",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party, party2))) =>
    val transactionsToSubmit = 10
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      responses <- ledger.completions(
        ledger.maxOffsetCheckpointEmissionDelay * NonNegativeInt.tryCreate(3),
        ledger
          .completionStreamRequest(endOffsetAtTestStart)(party2),
      )
      completions: Vector[Completion] = responses.flatMap(_.completion)
      checkpoints: Vector[OffsetCheckpoint] = responses.flatMap(_.offsetCheckpoint)
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        responses.nonEmpty,
        s"At least one response (checkpoint) should have been received but none were instead",
      )
      assert(
        completions.isEmpty,
        s"No completions should have been received but ${completions.size} were instead",
      )
      assert(
        checkpoints.nonEmpty,
        s"At least one checkpoint should have been received but none were instead",
      )
      assertCompletionsInOrder(responses)
    }
  })

  test(
    "CompletionsStreamCheckpointEmpty",
    "Command completions streams should contain a checkpoint message if there are any offset updates (even if they are for other parties)",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party, party2))) =>
    val transactionsToSubmit = 10
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      // sleep for 3 * maxOffsetCheckpointEmissionDelay to ensure that the offset checkpoint cache is updated
      _ <- Future(Thread.sleep(3 * ledger.maxOffsetCheckpointEmissionDelay.duration.toMillis))
      // fetching completions for party2 should return 0 completions
      responses <- ledger.completions(
        within,
        ledger
          .completionStreamRequest(endOffsetAtTestStart)(party2),
      )
      completions: Vector[Completion] = responses.flatMap(_.completion)
      checkpoints: Vector[OffsetCheckpoint] = responses.flatMap(_.offsetCheckpoint)
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        responses.nonEmpty,
        s"At least one response should have been received but none were instead",
      )
      assert(
        completions.isEmpty,
        s"No completions should have been received but ${completions.size} were instead",
      )
      assert(
        checkpoints.nonEmpty,
        s"At least one checkpoint should have been received but none were instead",
      )
      assertCompletionsInOrder(responses)
    }
  })

}

object CheckpointInTailingStreamsIT {
  private val within: NonNegativeFiniteDuration = NonNegativeFiniteDuration.tryOfSeconds(1L)

  sealed trait ElementOrCheckpoint
  final case object Empty extends ElementOrCheckpoint
  final case class Element(offset: Long) extends ElementOrCheckpoint
  final case class Checkpoint(offset: Long) extends ElementOrCheckpoint

  private def getElementOrCheckpoint(update: GetUpdatesResponse.Update): ElementOrCheckpoint =
    update match {
      case GetUpdatesResponse.Update.Empty => Empty
      case GetUpdatesResponse.Update.Transaction(tx) => Element(tx.offset)
      case GetUpdatesResponse.Update.Reassignment(r) => Element(r.offset)
      case GetUpdatesResponse.Update.OffsetCheckpoint(checkpoint) => Checkpoint(checkpoint.offset)
      case GetUpdatesResponse.Update.TopologyTransaction(topology) => Element(topology.offset)
    }

  private def getElementOrCheckpoint(completion: CompletionResponse): ElementOrCheckpoint =
    completion match {
      case CompletionResponse.Empty => Empty
      case CompletionResponse.Completion(completion) => Element(completion.offset)
      case CompletionResponse.OffsetCheckpoint(checkpoint) =>
        Checkpoint(checkpoint.offset)
    }

  private def assertUpdatesInOrder[T](updates: Seq[T], convert: T => ElementOrCheckpoint): Unit =
    updates.map(convert).foldLeft(0L) { (lastOffset, update) =>
      update match {
        case Empty => lastOffset
        case Element(offset) =>
          assert(
            offset > lastOffset,
            s"element with offset $offset should have been greater than last offset",
          )
          offset
        case Checkpoint(checkpointOffset) =>
          val checkpointOffsetStr = checkpointOffset
          assert(
            checkpointOffsetStr >= lastOffset,
            s"checkpoint with offset $checkpointOffset should have been greater or equal to last offset",
          )
          checkpointOffsetStr
      }
    }

  private def assertUpdatesInOrder(updates: Seq[GetUpdatesResponse.Update]): Unit =
    assertUpdatesInOrder[GetUpdatesResponse.Update](updates, getElementOrCheckpoint)

  private def assertCompletionsInOrder(completions: Seq[CompletionResponse]): Unit =
    assertUpdatesInOrder[CompletionResponse](completions, getElementOrCheckpoint)

}
