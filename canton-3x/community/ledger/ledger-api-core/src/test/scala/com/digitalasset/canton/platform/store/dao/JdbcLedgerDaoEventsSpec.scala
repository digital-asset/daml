// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers}
import com.digitalasset.canton.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement, OptionValues}

import scala.collection.immutable.Set
import scala.concurrent.Future

private[dao] trait JdbcLedgerDaoEventsSpec extends LoneElement with Inside with OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private def toOption(protoString: String): Option[String] = {
    if (protoString.nonEmpty) Some(protoString) else None
  }

  private def eventsReader = ledgerDao.eventsReader

  private def globalKeyWithMaintainers(value: String) = GlobalKeyWithMaintainers(
    GlobalKey.assertBuild(
      someTemplateId,
      someContractKey(alice, value),
      shared = true,
    ),
    Set(alice),
  )

  behavior of "JdbcLedgerDao (events)"

  it should "lookup a create event by contract id" in {

    for {
      (_, tx) <- store(singleCreate(cId => create(cId)))
      flatTx <- ledgerDao.transactionsReader.lookupFlatTransactionById(
        tx.transactionId,
        tx.actAs.toSet,
      )
      result <- eventsReader.getEventsByContractId(
        nonTransient(tx).loneElement,
        Set(alice),
      )
    } yield {
      val expected = flatTx.value.transaction.value.events.loneElement.event.created.value
      val actual = result.created.flatMap(_.createdEvent).value
      actual shouldBe expected
    }
  }

  it should "lookup an archive event by contract id" in {
    for {
      (_, tx1) <- store(singleCreate(cId => create(cId)))
      contractId = nonTransient(tx1).loneElement
      (_, tx2) <- store(singleExercise(contractId))
      flatTx <- ledgerDao.transactionsReader.lookupFlatTransactionById(
        tx2.transactionId,
        tx2.actAs.toSet,
      )
      expected = flatTx.value.transaction.value.events.loneElement.event.archived.value
      result <- eventsReader.getEventsByContractId(contractId, Set(alice))
    } yield {
      val actual = result.archived.flatMap(_.archivedEvent).value
      actual shouldBe expected
    }
  }

  it should "make events visible to signatories and observers (stakeholders) only" in {

    for {
      (_, tx) <- store(
        singleCreate(cId => create(cId, signatories = Set(alice), observers = Set(charlie)))
      )
      _ <- ledgerDao.transactionsReader.lookupTransactionTreeById(
        tx.transactionId,
        tx.actAs.toSet,
      )
      cId = nonTransient(tx).loneElement
      aliceView <- eventsReader.getEventsByContractId(cId, Set(alice))
      charlieView <- eventsReader.getEventsByContractId(cId, Set(charlie))
      emmaView <- eventsReader.getEventsByContractId(cId, Set(emma))
    } yield {
      aliceView.created.flatMap(_.createdEvent).isDefined shouldBe true
      charlieView.created.flatMap(_.createdEvent).isDefined shouldBe true
      emmaView.created.flatMap(_.createdEvent).isDefined shouldBe false
    }
  }

  it should "be able to lookup a create event by contract key" in {
    val key = globalKeyWithMaintainers("key0")
    for {
      (_, tx) <- store(singleCreate(cId => create(cId).copy(keyOpt = Some(key))))
      flatTx <- ledgerDao.transactionsReader.lookupFlatTransactionById(
        tx.transactionId,
        tx.actAs.toSet,
      )
      expected = flatTx.value.transaction.value.events.loneElement.event.created.value
      result <- eventsReader.getEventsByContractKey(
        contractKey = key.value,
        templateId = someTemplateId,
        requestingParties = Set(alice),
        endExclusiveSeqId = None,
        maxIterations = 1000,
      )
    } yield {
      val actual = result.createEvent.value
      actual shouldBe expected
    }
  }

  it should "lookup an archive event by contract key" in {
    val key = globalKeyWithMaintainers("key2")
    for {
      (_, tx1) <- store(singleCreate(cId => create(cId).copy(keyOpt = Some(key))))
      (_, tx2) <- store(singleExercise(nonTransient(tx1).loneElement))
      flatTx <- ledgerDao.transactionsReader.lookupFlatTransactionById(
        tx2.transactionId,
        tx2.actAs.toSet,
      )
      expected = flatTx.value.transaction.value.events.loneElement.event.archived.value
      result <- eventsReader.getEventsByContractKey(
        contractKey = key.value,
        templateId = someTemplateId,
        requestingParties = Set(alice),
        endExclusiveSeqId = None,
        maxIterations = 1000,
      )
    } yield {
      val actual = result.archiveEvent.value
      actual shouldBe expected
    }
  }

  private def createAndExerciseKey(key: GlobalKeyWithMaintainers, signatories: Set[Party]) = {
    for {
      (_, tx) <- store(
        singleCreate(cId => create(cId, signatories = signatories).copy(keyOpt = Some(key)))
      )
      (_, _) <- store(singleExercise(nonTransient(tx).loneElement))
    } yield tx
  }

  it should "return the maximum create prior to the end-exclusive-seq-id" in {
    val key = globalKeyWithMaintainers("key5")

    // (contract-id, continuation-token)
    def getNextResult(
        endExclusiveSeqId: Option[EventSequentialId]
    ): Future[(Option[String], Option[EventSequentialId])] = {
      eventsReader
        .getEventsByContractKey(
          contractKey = key.value,
          templateId = someTemplateId,
          requestingParties = Set(bob),
          endExclusiveSeqId = endExclusiveSeqId,
          maxIterations = 1000,
        )
        .map(r => (r.createEvent.map(_.contractId), toOption(r.continuationToken).map(_.toLong)))
    }

    for {
      tx1 <- createAndExerciseKey(key, Set(alice, bob))
      _ <- createAndExerciseKey(key, Set(alice))
      tx3 <- createAndExerciseKey(key, Set(alice, bob))
      (cId3, eventId3) <- getNextResult(None)
      (cId1, eventId1) <- getNextResult(eventId3) // event2 skipped
      (cId0, eventId0) <- getNextResult(eventId1)
    } yield {
      (cId3, eventId3.isDefined) shouldBe (nonTransient(tx3).headOption.map(_.coid), true)
      (cId1, eventId1.isDefined) shouldBe (nonTransient(tx1).headOption.map(_.coid), true)
      (cId0, eventId0.isDefined) shouldBe (None, false)
    }
  }

  it should "limit iterations when searching for contract key" in {
    val key = globalKeyWithMaintainers("key6")

    // (contract-defined, continuation-token)
    def getNextImmediateResult(
        endExclusiveSeqId: Option[EventSequentialId]
    ): Future[(Boolean, Option[EventSequentialId])] = {
      eventsReader
        .getEventsByContractKey(
          contractKey = key.value,
          templateId = someTemplateId,
          requestingParties = Set(bob),
          endExclusiveSeqId = endExclusiveSeqId,
          maxIterations = 1, // Only search ahead one iteration
        )
        .map(r => (r.createEvent.isDefined, toOption(r.continuationToken).map(_.toLong)))
    }

    for {
      _ <- createAndExerciseKey(key, Set(alice, bob))
      _ <- createAndExerciseKey(key, Set(alice))
      _ <- createAndExerciseKey(key, Set(alice, bob))
      (hasContract3, event3) <- getNextImmediateResult(None)
      (hasContract2, event2) <- getNextImmediateResult(event3)
      (hasContract1, event1) <- getNextImmediateResult(event2)
      (hasContract0, event0) <- getNextImmediateResult(event1)
    } yield {
      (hasContract3, event3.isDefined) shouldBe (true, true)
      (
        hasContract2,
        event2.isDefined,
      ) shouldBe (false, true) // Contact not visible but still more results
      (hasContract1, event1.isDefined) shouldBe (true, true)
      (hasContract0, event0.isDefined) shouldBe (false, false) // No more contracts or results
    }
  }

}
