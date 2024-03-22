// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.Node
import com.daml.lf.value.Value
import com.daml.platform.store.entries.LedgerEntry
import com.daml.platform.store.interfaces.TransactionLogUpdate
import org.scalatest._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

private[dao] trait JdbcLedgerDaoTransactionLogUpdatesSpec
    extends OptionValues
    with Inside
    with LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (getTransactionLogUpdates)"

  it should "return only the ledger end marker if no new transactions in range" in {
    for {
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
      result <- transactionsOf(
        ledgerDao.transactionsReader
          .getTransactionLogUpdates(
            startExclusive = ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId,
            endInclusive = ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId,
          )
      )
    } yield {
      result should contain theSameElementsAs Seq(
        TransactionLogUpdate.LedgerEndMarker(ledgerEnd.lastOffset, ledgerEnd.lastEventSeqId)
      )
      Succeeded
    }
  }

  it should "return the correct transaction log updates" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (createOffset, createTx) = singleCreate
      (offset1, t1) <- store(createOffset -> noSubmitterInfo(createTx))
      (offset2, t2) <- store(txCreateContractWithKey(alice, "some-key"))
      (offset3, t3) <- store(
        singleExercise(
          nonTransient(t2).loneElement,
          Some(Node.KeyWithMaintainers(someContractKey(alice, "some-key"), Set(alice))),
        )
      )
      (offset4, t4) <- store(fullyTransient())
      (offset5, t5) <- store(singleCreate)
      (offset6, t6) <- store(singleNonConsumingExercise(nonTransient(t5).loneElement))
      to <- ledgerDao.lookupLedgerEnd()
      result <- transactionsOf(
        ledgerDao.transactionsReader
          .getTransactionLogUpdates(
            startExclusive = from.lastOffset -> from.lastEventSeqId,
            endInclusive = to.lastOffset -> to.lastEventSeqId,
          )
      )
    } yield {
      val (
        Seq(
          actualTx1: TransactionLogUpdate.Transaction,
          actualTx2: TransactionLogUpdate.Transaction,
          actualTx3: TransactionLogUpdate.Transaction,
          actualTx4: TransactionLogUpdate.Transaction,
          actualTx5: TransactionLogUpdate.Transaction,
          actualTx6: TransactionLogUpdate.Transaction,
        ),
        Seq(endMarker: TransactionLogUpdate.LedgerEndMarker),
      ) = result.splitAt(6)

      val contractKey =
        t2.transaction.nodes.head._2.asInstanceOf[Node.Create].key.get.key
      val exercisedContractKey = Map(offset2 -> contractKey, offset3 -> contractKey)

      val eventSequentialIdGen = new AtomicLong(from.lastEventSeqId + 1L)

      assertExpectedEquality(actualTx1, t1, offset1, exercisedContractKey, eventSequentialIdGen)
      assertExpectedEquality(actualTx2, t2, offset2, exercisedContractKey, eventSequentialIdGen)
      assertExpectedEquality(actualTx3, t3, offset3, exercisedContractKey, eventSequentialIdGen)
      assertExpectedEquality(actualTx4, t4, offset4, exercisedContractKey, eventSequentialIdGen)
      assertExpectedEquality(actualTx5, t5, offset5, exercisedContractKey, eventSequentialIdGen)
      assertExpectedEquality(actualTx6, t6, offset6, exercisedContractKey, eventSequentialIdGen)

      endMarker.eventOffset shouldBe to.lastOffset
      endMarker.eventSequentialId shouldBe to.lastEventSeqId
    }
  }

  private def assertExpectedEquality(
      actual: TransactionLogUpdate.Transaction,
      expected: LedgerEntry.Transaction,
      expectedOffset: Offset,
      exercisedContractKey: Map[Offset, Value],
      eventSequentialIdRef: AtomicLong,
  ): Unit = {
    actual.transactionId shouldBe expected.transactionId
    actual.workflowId shouldBe expected.workflowId.value
    actual.effectiveAt shouldBe expected.ledgerEffectiveTime
    actual.offset shouldBe expectedOffset

    val actualEventsById = actual.events.map(ev => ev.eventId -> ev).toMap

    actualEventsById.size shouldBe expected.transaction.nodes.size

    expected.transaction.nodes.toVector.sortBy(_._1.index).foreach { case (nodeId, value) =>
      value match {
        case nodeCreate: Node.Create =>
          val expectedEventId = EventId(expected.transactionId, nodeId)
          val Some(actualCreated: TransactionLogUpdate.CreatedEvent) =
            actualEventsById.get(expectedEventId)
          actualCreated.contractId shouldBe nodeCreate.coid
          actualCreated.templateId shouldBe nodeCreate.templateId
          actualCreated.submitters should contain theSameElementsAs expected.actAs
            .map(_.toString)
            .toSet
          Ref.CommandId.fromString(actualCreated.commandId).toOption shouldBe expected.commandId
          actualCreated.treeEventWitnesses shouldBe nodeCreate.informeesOfNode
          actualCreated.flatEventWitnesses shouldBe nodeCreate.stakeholders
          actualCreated.createSignatories shouldBe nodeCreate.signatories
          actualCreated.createObservers shouldBe (nodeCreate.stakeholders diff nodeCreate.signatories)
          actualCreated.createArgument.unversioned shouldBe nodeCreate.arg
          actualCreated.createAgreementText.value shouldBe nodeCreate.agreementText
          actualCreated.nodeIndex shouldBe nodeId.index
          actualCreated.eventSequentialId shouldBe eventSequentialIdRef.getAndIncrement()
        case nodeExercises: Node.Exercise =>
          val expectedEventId = EventId(expected.transactionId, nodeId)
          val Some(actualExercised: TransactionLogUpdate.ExercisedEvent) =
            actualEventsById.get(expectedEventId)

          actualExercised.contractId shouldBe nodeExercises.targetCoid
          actualExercised.templateId shouldBe nodeExercises.templateId
          actualExercised.submitters should contain theSameElementsAs expected.actAs
            .map(_.toString)
            .toSet
          Ref.CommandId
            .fromString(actualExercised.commandId)
            .toOption shouldBe expected.commandId
          if (actualExercised.consuming)
            actualExercised.flatEventWitnesses shouldBe nodeExercises.stakeholders
          else
            actualExercised.flatEventWitnesses shouldBe empty
          actualExercised.treeEventWitnesses shouldBe nodeExercises.informeesOfNode
          actualExercised.exerciseArgument.unversioned shouldBe nodeExercises.chosenValue
          actualExercised.exerciseResult.map(_.unversioned) shouldBe nodeExercises.exerciseResult
          actualExercised.consuming shouldBe nodeExercises.consuming
          actualExercised.choice shouldBe nodeExercises.choiceId
          actualExercised.children should contain theSameElementsAs nodeExercises.children
            .map(_.toString)
            .toIndexedSeq
          actualExercised.actingParties shouldBe nodeExercises.actingParties
          actualExercised.nodeIndex shouldBe nodeId.index
          actualExercised.contractKey.map(_.unversioned) shouldBe exercisedContractKey.get(
            actual.offset
          )
          actualExercised.eventSequentialId shouldBe eventSequentialIdRef.getAndIncrement()
        case Node.Rollback(_) => ()
        case _ => ()
      }
    }
    ()
  }

  private def transactionsOf(
      source: Source[((Offset, Long), TransactionLogUpdate), NotUsed]
  ): Future[Seq[TransactionLogUpdate]] =
    source
      .map(_._2)
      .runWith(Sink.seq)
}
