// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import com.digitalasset.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.EventId
import com.digitalasset.platform.ApiOffset
import com.digitalasset.platform.events.EventIdFormatter.split
import org.scalatest._

private[dao] trait JdbcLedgerDaoTransactionTreesSpec
    extends OptionValues
    with Inside
    with LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (lookupTransactionTreeById)"

  it should "return nothing for a mismatching transaction id" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.transactionsReader
        .lookupTransactionTreeById(transactionId = "WRONG", Set(tx.submittingParty.get))
    } yield {
      result shouldBe None
    }
  }

  it should "return nothing for a mismatching party" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.transactionsReader
        .lookupTransactionTreeById(tx.transactionId, Set("WRONG"))
    } yield {
      result shouldBe None
    }
  }

  it should "return the expected transaction tree for a correct request (create)" in {
    for {
      (offset, tx) <- store(singleCreate)
      result <- ledgerDao.transactionsReader
        .lookupTransactionTreeById(tx.transactionId, Set(tx.submittingParty.get))
    } yield {
      inside(result.value.transaction) {
        case Some(transaction) =>
          val (eventId, createNode: NodeCreate.WithTxValue[AbsoluteContractId]) =
            tx.transaction.nodes.head
          transaction.commandId shouldBe tx.commandId.get
          transaction.offset shouldBe ApiOffset.toApiString(offset)
          transaction.effectiveAt.value.seconds shouldBe tx.ledgerEffectiveTime.getEpochSecond
          transaction.effectiveAt.value.nanos shouldBe tx.ledgerEffectiveTime.getNano
          transaction.transactionId shouldBe tx.transactionId
          transaction.workflowId shouldBe tx.workflowId.getOrElse("")
          val created = transaction.eventsById.values.loneElement.getCreated
          transaction.rootEventIds.loneElement shouldEqual created.eventId
          created.eventId shouldBe eventId
          created.witnessParties should contain only tx.submittingParty.get
          created.agreementText.getOrElse("") shouldBe createNode.coinst.agreementText
          created.contractKey shouldBe None
          created.createArguments shouldNot be(None)
          created.signatories should contain theSameElementsAs createNode.signatories
          created.observers should contain theSameElementsAs createNode.stakeholders.diff(
            createNode.signatories)
          created.templateId shouldNot be(None)
      }
    }
  }

  it should "return the expected transaction tree for a correct request (exercise)" in {
    for {
      (_, create) <- store(singleCreate)
      (offset, exercise) <- store(singleExercise(nonTransient(create).loneElement))
      result <- ledgerDao.transactionsReader
        .lookupTransactionTreeById(exercise.transactionId, Set(exercise.submittingParty.get))
    } yield {
      inside(result.value.transaction) {
        case Some(transaction) =>
          val (eventId, exerciseNode: NodeExercises.WithTxValue[EventId, AbsoluteContractId]) =
            exercise.transaction.nodes.head
          transaction.commandId shouldBe exercise.commandId.get
          transaction.offset shouldBe ApiOffset.toApiString(offset)
          transaction.effectiveAt.value.seconds shouldBe exercise.ledgerEffectiveTime.getEpochSecond
          transaction.effectiveAt.value.nanos shouldBe exercise.ledgerEffectiveTime.getNano
          transaction.transactionId shouldBe exercise.transactionId
          transaction.workflowId shouldBe exercise.workflowId.getOrElse("")
          val exercised = transaction.eventsById.values.loneElement.getExercised
          transaction.rootEventIds.loneElement shouldEqual exercised.eventId
          exercised.eventId shouldBe eventId
          exercised.witnessParties should contain only exercise.submittingParty.get
          exercised.contractId shouldBe exerciseNode.targetCoid.coid
          exercised.templateId shouldNot be(None)
          exercised.actingParties should contain theSameElementsAs exerciseNode.actingParties
          exercised.childEventIds shouldBe Seq.empty
          exercised.choice shouldBe exerciseNode.choiceId
          exercised.choiceArgument shouldNot be(None)
          exercised.consuming shouldBe true
          exercised.exerciseResult shouldNot be(None)
      }
    }
  }

  it should "return the expected transaction tree for a correct request (create, exercise)" in {
    for {
      (offset, tx) <- store(fullyTransient)
      result <- ledgerDao.transactionsReader
        .lookupTransactionTreeById(tx.transactionId, Set(tx.submittingParty.get))
    } yield {
      inside(result.value.transaction) {
        case Some(transaction) =>
          val (createEventId, createNode) =
            tx.transaction.nodes.collectFirst {
              case (eventId, node: NodeCreate.WithTxValue[AbsoluteContractId]) =>
                eventId -> node
            }.get
          val (exerciseEventId, exerciseNode) =
            tx.transaction.nodes.collectFirst {
              case (eventId, node: NodeExercises.WithTxValue[EventId, AbsoluteContractId]) =>
                eventId -> node
            }.get

          transaction.commandId shouldBe tx.commandId.get
          transaction.offset shouldBe ApiOffset.toApiString(offset)
          transaction.transactionId shouldBe tx.transactionId
          transaction.workflowId shouldBe tx.workflowId.getOrElse("")
          transaction.effectiveAt.value.seconds shouldBe tx.ledgerEffectiveTime.getEpochSecond
          transaction.effectiveAt.value.nanos shouldBe tx.ledgerEffectiveTime.getNano

          transaction.rootEventIds should have size 2
          transaction.rootEventIds(0) shouldBe createEventId
          transaction.rootEventIds(1) shouldBe exerciseEventId

          val created = transaction.eventsById(createEventId).getCreated
          val exercised = transaction.eventsById(exerciseEventId).getExercised

          created.eventId shouldBe createEventId
          created.witnessParties should contain only tx.submittingParty.get
          created.agreementText.getOrElse("") shouldBe createNode.coinst.agreementText
          created.contractKey shouldBe None
          created.createArguments shouldNot be(None)
          created.signatories should contain theSameElementsAs createNode.signatories
          created.observers should contain theSameElementsAs createNode.stakeholders.diff(
            createNode.signatories)
          created.templateId shouldNot be(None)

          exercised.eventId shouldBe exerciseEventId
          exercised.witnessParties should contain only tx.submittingParty.get
          exercised.contractId shouldBe exerciseNode.targetCoid.coid
          exercised.templateId shouldNot be(None)
          exercised.actingParties should contain theSameElementsAs exerciseNode.actingParties
          exercised.childEventIds shouldBe Seq.empty
          exercised.choice shouldBe exerciseNode.choiceId
          exercised.choiceArgument shouldNot be(None)
          exercised.consuming shouldBe true
          exercised.exerciseResult shouldNot be(None)
      }
    }
  }

  it should "return a transaction tree with the expected shape for a partially visible transaction" in {
    for {
      (_, tx) <- store(withChildren)
      result <- ledgerDao.transactionsReader
        .lookupTransactionTreeById(tx.transactionId, Set("Alice")) // only two children are visible to Alice
    } yield {
      inside(result.value.transaction) {
        case Some(transaction) =>
          val createEventId =
            tx.transaction.nodes.collectFirst {
              case (eventId, _) if split(eventId).exists(_.nodeId.index == 2) => eventId
            }.get
          val exerciseEventId =
            tx.transaction.nodes.collectFirst {
              case (eventId, _) if split(eventId).exists(_.nodeId.index == 3) => eventId
            }.get

          transaction.eventsById should have size 2

          transaction.rootEventIds should have size 2
          transaction.rootEventIds(0) shouldBe createEventId
          transaction.rootEventIds(1) shouldBe exerciseEventId
      }
    }
  }

}
