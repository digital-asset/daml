// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.offset.Offset
import com.daml.lf.transaction.Node
import com.daml.lf.value.Value
import com.daml.platform.ApiOffset
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement, OptionValues}

import scala.concurrent.Future

private[dao] trait JdbcLedgerDaoEventsSpec extends LoneElement with Inside with OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private def eventsReader = ledgerDao.eventsReader

  behavior of "JdbcLedgerDao (events)"

  it should "be able to lookup a create event by contract id" in {

    for {
      (_, tx) <- store(singleCreate(cId => create(cId)))
      flatTx <- ledgerDao.transactionsReader.lookupTransactionTreeById(
        tx.transactionId,
        tx.actAs.toSet,
      )
      result <- eventsReader.getEventsByContractId(
        nonTransient(tx).loneElement,
        Set(alice),
      )
    } yield {
      val expected = flatTx.value.transaction.value.eventsById.values.loneElement.kind.created.value
      val actual = result.events.loneElement.kind.created.value
      actual shouldBe expected
    }
  }

  it should "be able to lookup a create event by contract key" in {
    val key = Node.KeyWithMaintainers(someContractKey(alice, "key0"), Set(alice))
    for {
      (offset, tx) <- store(singleCreate(cId => create(cId).copy(key = Some(key))))
      treeTx <- ledgerDao.transactionsReader.lookupTransactionTreeById(
        tx.transactionId,
        tx.actAs.toSet,
      )
      expected = treeTx.value.transaction.value.eventsById.values.loneElement.kind.created.value
      result <- eventsReader.getEventsByContractKey(
        contractKey = key.key,
        templateId = someTemplateId,
        requestingParties = Set(alice),
        maxEvents = 10,
        startExclusive = Offset.beforeBegin,
        endInclusive = offset,
      )
    } yield {
      val actual = result.events.loneElement.kind.created.value
      actual shouldBe expected
    }
  }

  it should "lookup an exercise event by contract key" in {
    val key = Node.KeyWithMaintainers(someContractKey(alice, "key2"), Set(alice))
    for {
      (_, tx1) <- store(singleCreate(cId => create(cId).copy(key = Some(key))))
      (lastOffset, tx2) <- store(singleExercise(nonTransient(tx1).loneElement))
      treeTx <- ledgerDao.transactionsReader.lookupTransactionTreeById(
        tx2.transactionId,
        tx2.actAs.toSet,
      )
      expected = treeTx.value.transaction.value.eventsById.values.loneElement.kind.exercised.value
      result <- eventsReader.getEventsByContractKey(
        contractKey = key.key,
        templateId = someTemplateId,
        requestingParties = Set(alice),
        maxEvents = 10,
        startExclusive = Offset.beforeBegin,
        endInclusive = lastOffset,
      )
    } yield {
      val actual = result.events.drop(1).loneElement.kind.exercised.value
      actual shouldBe expected
    }
  }

  it should "only return a maximum number of events" in {
    val key = Node.KeyWithMaintainers(someContractKey(alice, "key3"), Set(alice))
    for {
      (_, tx1) <- store(singleCreate(cId => create(cId).copy(key = Some(key))))
      (_, _) <- store(singleExercise(nonTransient(tx1).loneElement))
      (_, tx2) <- store(singleCreate(cId => create(cId).copy(key = Some(key))))
      (lastOffset, _) <- store(singleExercise(nonTransient(tx2).loneElement))
      expected = 3
      result <- eventsReader.getEventsByContractKey(
        contractKey = key.key,
        templateId = someTemplateId,
        requestingParties = Set(alice),
        maxEvents = expected,
        startExclusive = Offset.beforeBegin,
        endInclusive = lastOffset,
      )
    } yield {
      val actual = result.events.size
      actual shouldBe expected
    }
  }

  it should "only return events within the given offset range" in {
    val key = Node.KeyWithMaintainers(someContractKey(alice, "key4"), Set(alice))

    def hasCid(
        cId: Value.ContractId,
        startExclusive: Offset,
        endInclusive: Offset,
    ): Future[Boolean] = {
      eventsReader
        .getEventsByContractKey(
          contractKey = key.key,
          templateId = someTemplateId,
          requestingParties = Set(alice),
          maxEvents = 10,
          startExclusive = startExclusive,
          endInclusive = endInclusive,
        )
        .map(_.events.exists(_.kind.created.exists(_.contractId == cId.coid)))
    }

    for {
      (offset1, tx1) <- store(singleCreate(cId => create(cId).copy(key = Some(key))))
      (_, _) <- store(singleExercise(nonTransient(tx1).loneElement))
      (offset2, tx2) <- store(singleCreate(cId => create(cId).copy(key = Some(key))))
      targetCid = nonTransient(tx2).loneElement
      (lastOffset, _) <- store(singleExercise(targetCid))
      firstOffset = Offset.beforeBegin

      hasKey1 <- hasCid(targetCid, startExclusive = firstOffset, endInclusive = lastOffset)
      hasKey2 <- hasCid(targetCid, offset2, offset2)
      hasKey3 <- hasCid(targetCid, startExclusive = firstOffset, endInclusive = offset2)
      hasKey4 <- hasCid(targetCid, startExclusive = firstOffset, endInclusive = offset1)
      hasKey5 <- hasCid(targetCid, startExclusive = offset1, endInclusive = lastOffset)
      hasKey6 <- hasCid(targetCid, startExclusive = offset2, endInclusive = lastOffset)
    } yield {
      assert(hasKey1, s"Unspecified offsets do not restrict selection")
      assert(!hasKey2, s"Matching from/to offset return no rows")
      assert(hasKey3, s"End offset on key is included")
      assert(!hasKey4, s"End offset before key is excluded")
      assert(hasKey5, s"Begin offset before key is included")
      assert(!hasKey6, s"Begin offset on key is excluded")
    }

  }

  it should "page results when there may be more than max events available" in {
    val key = Node.KeyWithMaintainers(someContractKey(alice, "key5"), Set(alice))

    def getNextTwoResults(beginExclusive: Offset, endExclusive: Offset): Future[Option[Offset]] = {
      eventsReader
        .getEventsByContractKey(
          contractKey = key.key,
          templateId = someTemplateId,
          requestingParties = Set(alice),
          maxEvents = 2,
          startExclusive = beginExclusive,
          endInclusive = endExclusive,
        )
        .map(_.lastOffset.map(_.getAbsolute).map(ApiOffset.assertFromString))
    }

    for {
      (_, tx1) <- store(singleCreate(cId => create(cId).copy(key = Some(key))))
      (_, _) <- store(singleExercise(nonTransient(tx1).loneElement))
      (_, tx2) <- store(singleCreate(cId => create(cId).copy(key = Some(key))))
      (_, _) <- store(singleExercise(nonTransient(tx2).loneElement))
      (lastOffset, _) <- store(singleCreate(cId => create(cId)))

      lastOffset1 <- getNextTwoResults(Offset.beforeBegin, lastOffset)
      lastOffset2 <- getNextTwoResults(lastOffset1.value, lastOffset)
      lastOffset3 <- getNextTwoResults(lastOffset2.value, lastOffset)
    } yield {
      lastOffset3 shouldBe None
    }
  }

}

final class JdbcLedgerDaoEventsH2Spec
    extends AsyncFlatSpec
    with Matchers
    with JdbcLedgerDaoSuite
    with JdbcLedgerDaoBackendH2Database
    with JdbcLedgerDaoPackagesSpec
    with JdbcLedgerDaoEventsSpec

final class JdbcLedgerDaoEventsPostgresSpec
    extends AsyncFlatSpec
    with Matchers
    with JdbcLedgerDaoSuite
    with JdbcLedgerDaoBackendPostgresql
    with JdbcLedgerDaoPackagesSpec
    with JdbcLedgerDaoEventsSpec

final class JdbcLedgerDaoEventsOracleSpec
    extends AsyncFlatSpec
    with Matchers
    with JdbcLedgerDaoSuite
    with JdbcLedgerDaoBackendOracle
    with JdbcLedgerDaoPackagesSpec
    with JdbcLedgerDaoEventsSpec
