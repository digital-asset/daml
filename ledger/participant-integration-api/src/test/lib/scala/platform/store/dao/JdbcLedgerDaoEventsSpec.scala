// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement, OptionValues}

import scala.concurrent.Future

private[dao] trait JdbcLedgerDaoEventsSpec extends LoneElement with Inside with OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private def eventsReader = ledgerDao.eventsReader

  private def globalKeyWithMaintainers(value: String) = GlobalKeyWithMaintainers(
    GlobalKey.assertBuild(someTemplateId, someContractKey(alice, value)),
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
      val actual = result.createEvent.value
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
      val actual = result.archiveEvent.value
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
      aliceView.createEvent.isDefined shouldBe true
      charlieView.createEvent.isDefined shouldBe true
      emmaView.createEvent.isDefined shouldBe false
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
        endExclusiveEventId = None,
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
        endExclusiveEventId = None,
      )
    } yield {
      val actual = result.archiveEvent.value
      actual shouldBe expected
    }
  }

  it should "return the maximum create prior to the end-exclusive-event" in {
    val key = globalKeyWithMaintainers("key5")

    def getNextResult(endExclusiveEventId: Option[EventId]): Future[Option[(String, EventId)]] = {
      eventsReader
        .getEventsByContractKey(
          contractKey = key.value,
          templateId = someTemplateId,
          requestingParties = Set(bob),
          endExclusiveEventId = endExclusiveEventId,
        )
        .map(_.createEvent.map(e => (e.contractId, EventId.fromString(e.eventId).toOption.value)))
    }

    for {
      (_, tx1) <- store(
        singleCreate(cId => create(cId, signatories = Set(alice, bob)).copy(keyOpt = Some(key)))
      )
      (_, _) <- store(singleExercise(nonTransient(tx1).loneElement))
      (_, tx2) <- store(
        singleCreate(cId => create(cId, signatories = Set(alice)).copy(keyOpt = Some(key)))
      )
      (_, _) <- store(singleExercise(nonTransient(tx2).loneElement))
      (_, tx3) <- store(
        singleCreate(cId => create(cId, signatories = Set(alice, bob)).copy(keyOpt = Some(key)))
      )

      Some((cId3, eventId3)) <- getNextResult(None)
      // Event 2 should be skipped as bob has no visibility of it
      Some((cId1, eventId1)) <- getNextResult(Some(eventId3))
      maybeEventId0 <- getNextResult(Some(eventId1))
    } yield {
      cId3 shouldBe nonTransient(tx3).loneElement.coid
      cId1 shouldBe nonTransient(tx1).loneElement.coid
      maybeEventId0 shouldBe None
    }
  }

}
