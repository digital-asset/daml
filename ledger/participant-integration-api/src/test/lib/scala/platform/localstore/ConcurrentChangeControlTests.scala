// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import java.sql.Connection
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors}

import com.daml.logging.LoggingContext
import com.daml.metrics.DatabaseMetrics
import com.daml.platform.store.backend.{ResourceVersionOpsBackend, StorageBackendProvider}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future}

trait ConcurrentChangeControlTests extends PersistentStoreSpecBase with Matchers {
  self: AsyncFreeSpec with StorageBackendProvider =>

  private[localstore] def testedResourceVersionBackend: ResourceVersionOpsBackend

  private[localstore] type ResourceId
  private[localstore] type DbResource

  private[localstore] def createAndGetNewResource(initialResourceVersion: Long)(
      connection: Connection
  ): DbResource

  private[localstore] def fetchResourceVersion(id: ResourceId)(connection: Connection): Long

  private[localstore] def getResourceVersion(resource: DbResource): Long

  private[localstore] def getId(resource: DbResource): ResourceId

  private[localstore] def getDbInternalId(resource: DbResource): Int

  "concurrent change control primitives" - {

    "comparing and increasing resource version should block slower updater-transaction. only faster transaction does a successful update" in {
      for {
        updateResults <- whenTwoConcurrentTransactionsUpdateTheSameRowFixture(
          updateRowFun = (userInternalId, expectedResourceVersion) =>
            connection =>
              testedResourceVersionBackend.compareAndIncreaseResourceVersion(
                userInternalId,
                expectedResourceVersion,
              )(connection)
        )
      } yield updateResults shouldBe List(101 -> true, 101 -> false)

    }

    "increasing resource version should block slower updater-transaction. both transactions do a successful update" in {
      for {
        updateResults <- whenTwoConcurrentTransactionsUpdateTheSameRowFixture(
          updateRowFun = (userInternalId, _) =>
            connection =>
              testedResourceVersionBackend.increaseResourceVersion(
                userInternalId
              )(connection)
        )
      } yield updateResults shouldBe List(101 -> true, 102 -> true)
    }

  }

  private def whenTwoConcurrentTransactionsUpdateTheSameRowFixture(
      updateRowFun: (Int, Long) => (Connection) => Boolean
  ): Future[List[(Long, Boolean)]] = {
    import scala.jdk.CollectionConverters._

    val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

    val eventsQueue = new ConcurrentLinkedQueue[String]()
    // Latches to coordinate the initial state
    val barStarted = new CountDownLatch(1)
    val fooIssuedUpdateQuery = new CountDownLatch(1)
    val barIsAboutToIssueUpdateQuery = new CountDownLatch(1)
    // Latches to coordinate an assertion while both transaction are still in-flight.
    val updateStatementInFooDone = new CountDownLatch(1)
    val externalCheckDone = new CountDownLatch(1)

    for {
      // create a user with a known resource version
      resource <- inTransaction { connection =>
        createAndGetNewResource(initialResourceVersion = 100)(connection)
      }
      _ = getResourceVersion(resource) shouldBe 100
      txA = Future {
        barStarted.await()
      }(ec).flatMap(_ =>
        inTransaction { connection =>
          eventsQueue.add("Foo0")
          val updateSucceeded = updateRowFun(getDbInternalId(resource), 100)(connection)
          fooIssuedUpdateQuery.countDown()
          val resourceVersion = fetchResourceVersion(id = getId(resource))(connection)
          barIsAboutToIssueUpdateQuery.await()
          eventsQueue.add("Foo1")
          updateStatementInFooDone.countDown()
          externalCheckDone.await()
          resourceVersion -> updateSucceeded
        }
      )(ec)
      txB = inTransaction { connection =>
        eventsQueue.add("Bar0")
        barStarted.countDown()
        // NOTE: This is the only countdown latch Bar is waiting on
        fooIssuedUpdateQuery.await()
        eventsQueue.add("Bar1")
        barIsAboutToIssueUpdateQuery.countDown()
        val updateSucceeded = updateRowFun(getDbInternalId(resource), 100)(connection)
        eventsQueue.add("Bar2")
        val resourceVersion = fetchResourceVersion(id = getId(resource))(connection)
        resourceVersion -> updateSucceeded
      }
      _ = updateStatementInFooDone.await()
      // NOTE: Sequence is of events {"Bar0", "Foo0", "Bar1"} represents an initial state guaranteed by the countdown latch setup.
      //       It means both Foo and Bar transaction are in progress and Foo additionally completed an update query.
      // NOTE: The thing we are testing for is that Bar cannot finish before Foo, because
      //       both wrote to the same row and Foo did it faster.
      _ = eventsQueue.iterator().asScala.toList shouldBe List("Bar0", "Foo0", "Bar1", "Foo1")
      // NOTE: Here we sleep to to give an extra time for Bar to finish to prevent false positives where Bar was simply very slow, rather than being blocked by the DBMS.
      _ = Thread.sleep(100)
      _ = eventsQueue.iterator().asScala.toList shouldBe List("Bar0", "Foo0", "Bar1", "Foo1")
      _ = externalCheckDone.countDown()
      updateResults <- Future.sequence(List(txA, txB))
      _ = eventsQueue.iterator().asScala.toList shouldBe List(
        "Bar0",
        "Foo0",
        "Bar1",
        "Foo1",
        "Bar2",
      )
    } yield {
      updateResults
    }
  }

  private def inTransaction[T](thunk: Connection => T): Future[T] = {
    dbSupport.dbDispatcher
      .executeSql(DatabaseMetrics.ForTesting("concurrent change control"))(thunk)(
        LoggingContext.ForTesting
      )
  }

}
