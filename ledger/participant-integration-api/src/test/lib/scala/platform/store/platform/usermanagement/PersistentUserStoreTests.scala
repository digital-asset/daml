// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.platform.usermanagement

import java.sql.Connection
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors}

import com.daml.api.util.TimeProvider
import com.daml.ledger.participant.localstore.UserStoreTests
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.{DatabaseMetrics, Metrics}
import com.daml.platform.store.PersistentStoreSpecBase
import com.daml.platform.store.backend.StorageBackendProvider
import com.daml.platform.store.backend.UserManagementStorageBackend.DbUserPayload
import com.daml.platform.store.backend.common.UserManagementStorageBackendImpl
import com.daml.platform.usermanagement.PersistentUserManagementStore
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.{ExecutionContext, Future}

trait PersistentUserStoreTests extends PersistentStoreSpecBase with UserStoreTests {
  self: AsyncFreeSpec with StorageBackendProvider =>

  override def newStore() = new PersistentUserManagementStore(
    dbSupport = dbSupport,
    metrics = Metrics.ForTesting,
    timeProvider = TimeProvider.UTC,
    maxRightsPerUser = 100,
  )

  private def inTransaction[T](thunk: Connection => T): Future[T] = {
    dbSupport.dbDispatcher
      .executeSql(DatabaseMetrics.ForTesting("concurrent change control"))(thunk)(
        LoggingContext.ForTesting
      )
  }

  // TODO um-for-hub: Test for both User and PartyRecord stores
  "concurrent change control primitives" - {

    def whenTwoConcurrentTransactionsUpdateTheSameRow(
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

      val userId = Ref.UserId.assertFromString("user1")
      for {
        // create a user with a known resource version
        user <- inTransaction { connection =>
          UserManagementStorageBackendImpl.createUser(
            DbUserPayload(
              id = userId,
              primaryPartyO = None,
              isDeactivated = false,
              resourceVersion = 100,
              createdAt = 0,
            )
          )(connection)
          UserManagementStorageBackendImpl.getUser(userId)(connection).value
        }
        getUserFun = (connection: Connection) => {
          UserManagementStorageBackendImpl
            .getUser(user.payload.id)(connection)
            .value
            .payload
            .resourceVersion
        }
        _ = user.payload.resourceVersion shouldBe 100
        txA = Future {
          barStarted.await()
        }(ec).flatMap(_ =>
          inTransaction { connection =>
            eventsQueue.add("Foo0")
            val updateSucceeded = updateRowFun(user.internalId, 100)(connection)
            fooIssuedUpdateQuery.countDown()
            val resourceVersion = getUserFun(connection)
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
          val updateSucceeded = updateRowFun(user.internalId, 100)(connection)
          eventsQueue.add("Bar2")
          val resourceVersion = getUserFun(connection)
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

    "comparing and increasing resource version should block slower updater-transaction. only faster transaction does a successful update" in {
      for {
        updateResults <- whenTwoConcurrentTransactionsUpdateTheSameRow(
          updateRowFun = (userInternalId, expectedResourceVersion) =>
            connection =>
              UserManagementStorageBackendImpl.compareAndIncreaseResourceVersion(
                userInternalId,
                expectedResourceVersion,
              )(connection)
        )
      } yield updateResults shouldBe List(101 -> true, 101 -> false)

    }

    "increasing resource version should block slower updater-transaction. both transactions do a successful update" in {
      for {
        updateResults <- whenTwoConcurrentTransactionsUpdateTheSameRow(
          updateRowFun = (userInternalId, _) =>
            connection =>
              UserManagementStorageBackendImpl.increaseResourceVersion(
                userInternalId
              )(connection)
        )
      } yield updateResults shouldBe List(101 -> true, 102 -> true)
    }

  }

}
