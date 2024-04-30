// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, MigrationMode, PostgresTest}
import com.digitalasset.canton.topology.UniqueIdentifier
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait InitializationStoreTest extends AsyncWordSpec with BaseTest {

  val uid = UniqueIdentifier.tryFromProtoPrimitive("da::default")
  val uid2 = UniqueIdentifier.tryFromProtoPrimitive("two::default")

  def myMigrationMode: MigrationMode

  def initializationStore(mk: () => InitializationStore): Unit = {
    "when storing the unique identifier" should {
      "be able to set the value of the id" in {
        val store = mk()
        for {
          emptyId <- store.uid
          _ = emptyId shouldBe None
          _ <- store.setUid(uid)
          id <- store.uid
        } yield id shouldBe Some(uid)
      }
      "fail when trying to set two different ids" in {
        val store = mk()
        for {
          _ <- store.setUid(uid)
          _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
            store.setUid(uid2),
            _.getMessage shouldBe s"Unique id of node is already defined as $uid and can't be changed to $uid2!",
          )
        } yield succeed
      }

      "support dev version" in {
        val store = mk()
        myMigrationMode match {
          case MigrationMode.Standard =>
            // query should fail with an exception
            store.throwIfNotDev.failed.map { _ =>
              succeed
            }
          case MigrationMode.DevVersion =>
            store.throwIfNotDev.map { _ shouldBe true }
        }
      }
    }
  }
}

trait DbInitializationStoreTest extends InitializationStoreTest {
  this: DbTest =>

  override def myMigrationMode: MigrationMode = migrationMode

  def cleanDb(storage: DbStorage): Future[Int] = {
    import storage.api.*
    storage.update(
      sqlu"truncate table common_node_id",
      operationName = s"${this.getClass}: truncate table common_node_id",
    )
  }

  "DbInitializationStore" should {
    behave like initializationStore(() =>
      new DbInitializationStore(storage, timeouts, loggerFactory)
    )
  }
}

class DbInitializationStoreTestH2 extends DbInitializationStoreTest with H2Test

class DbInitializationStoreTestPostgres extends DbInitializationStoreTest with PostgresTest

class InitializationStoreTestInMemory extends InitializationStoreTest {

  override def myMigrationMode: MigrationMode = MigrationMode.Standard

  "InMemoryInitializationStore" should {
    behave like initializationStore(() => new InMemoryInitializationStore(loggerFactory))
  }
}
