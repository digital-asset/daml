// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import cats.effect.IO
import com.daml.http.dbbackend.ConnectionPool.PoolSize
import com.daml.http.dbbackend.{ContractDao, JdbcConfig, SupportedJdbcDriver}
import com.daml.http.util.Logging.{InstanceUUID, instanceUUIDLogCtx}
import com.daml.logging.LoggingContextOf
import doobie.util.log
import doobie.util.log.LogHandler
import org.scalatest.freespec.AsyncFreeSpecLike
import org.scalatest.{Assertion, AsyncTestSuite, BeforeAndAfterAll, Inside}
import org.scalatest.matchers.should.Matchers

import scala.collection.compat._
import scala.concurrent.Future

abstract class AbstractDatabaseIntegrationTest extends AsyncFreeSpecLike with BeforeAndAfterAll {
  this: AsyncTestSuite with Matchers with Inside =>

  protected def jdbcConfig: JdbcConfig

  // has to be lazy because jdbcConfig is NOT initialized yet
  protected lazy val dao = dbbackend.ContractDao(
    jdbcConfig.copy(tablePrefix = "some_fancy_prefix_"),
    poolSize = PoolSize.Integration,
  )

  protected lazy val daoWithoutPrefix =
    dbbackend.ContractDao(jdbcConfig.copy(tablePrefix = ""), poolSize = PoolSize.Integration)

  override protected def afterAll(): Unit = {
    dao.close()
    daoWithoutPrefix.close()
    super.afterAll()
  }

  "DbStartupOps" - {

    import dao.{logHandler, jdbcDriver}, jdbcDriver.q.queries

    import doobie.implicits.toSqlInterpolator, DbStartupOps.DbVersionState._, DbStartupOps._,
    com.daml.http.dbbackend.DbStartupMode._

    implicit lazy val _dao: ContractDao = dao

    def withFreshDb(fun: LoggingContextOf[InstanceUUID] => IO[Assertion]): Future[Assertion] =
      dao
        .transact(queries.dropAllTablesIfExist)
        .flatMap(_ => fun(instanceUUIDLogCtx()))
        .unsafeToFuture()

    "fromStartupMode called with CreateIfNeededAndStart will re-initialize the cache when no schema version is available" in withFreshDb {
      implicit lc =>
        for {
          res1 <- fromStartupMode(dao, CreateIfNeededAndStart)
          _ = res1 shouldBe true
          version <- dao.transact(
            sql"SELECT version FROM ${queries.jsonApiSchemaVersionTableName}"
              .query[Int]
              .unique
          )
        } yield version shouldBe queries.schemaVersion
    }

    "getDbVersionState will return Missing when the schema version table is missing" in withFreshDb {
      implicit lc =>
        for {
          res1 <- getDbVersionState
        } yield res1 shouldBe Right(Missing)
    }

    "getDbVersionState will return Mismatch when the schema version exists but isn't equal to the current one" in withFreshDb {
      implicit lc =>
        val wrongVersion = -1
        for {
          res1 <- fromStartupMode(dao, CreateOnly)
          _ = res1 shouldBe true
          _ <- dao.transact(
            sql"DELETE FROM ${queries.jsonApiSchemaVersionTableName}".update.run
          )
          _ <- dao.transact(
            sql"INSERT INTO ${queries.jsonApiSchemaVersionTableName}(version) VALUES($wrongVersion)".update.run
          )
          res2 <- getDbVersionState
        } yield res2 shouldBe Right(Mismatch(queries.schemaVersion, wrongVersion))
    }

    "getDbVersionState will return UpToDate when the schema version exists and is equal to the current one" in withFreshDb {
      implicit lc =>
        for {
          res1 <- fromStartupMode(dao, CreateOnly)
          _ = res1 shouldBe true
          res2 <- getDbVersionState
        } yield res2 shouldBe Right(UpToDate)
    }

    "fromStartupMode called with StartOnly will succeed when the schema version exists and is equal to the current one" in withFreshDb {
      implicit lc =>
        for {
          res1 <- fromStartupMode(dao, CreateOnly)
          _ = res1 shouldBe true
          res2 <- fromStartupMode(dao, StartOnly)
          _ = res2 shouldBe true
          versions <- dao.transact(
            sql"SELECT version FROM ${queries.jsonApiSchemaVersionTableName}"
              .query[Int]
              .nel
          )
        } yield {
          Set.from(versions.toList) shouldBe Set(queries.schemaVersion)
        }
    }

    "fromStartupMode called with StartOnly does not succeed when no schema version is available" in withFreshDb {
      implicit lc =>
        fromStartupMode(dao, StartOnly)
          .map(res => res shouldBe false)
    }

    "fromStartupMode called with StartOnly does not succeed when the schema version exists but isn't equal to the current one" in withFreshDb {
      implicit lc =>
        for {
          res1 <- fromStartupMode(dao, CreateOnly)
          _ = res1 shouldBe true
          _ <- dao.transact(
            sql"DELETE FROM ${queries.jsonApiSchemaVersionTableName}".update.run
          )
          _ <- dao.transact(
            sql"INSERT INTO ${queries.jsonApiSchemaVersionTableName}(version) VALUES(-1)".update.run
          )
          res2 <- fromStartupMode(dao, StartOnly)
        } yield res2 shouldBe false
    }

    "fromStartupMode called with CreateOnly initializes the schema correctly when the db is fresh" in withFreshDb {
      implicit lc =>
        for {
          res1 <- fromStartupMode(dao, CreateOnly)
          _ = res1 shouldBe true
          versions <- dao.transact(
            sql"SELECT version FROM ${queries.jsonApiSchemaVersionTableName}"
              .query[Int]
              .nel
          )
        } yield {
          Set.from(versions.toList) shouldBe Set(queries.schemaVersion)
        }
    }

    "fromStartupMode called with CreateAndStart initializes the schema correctly when the db is fresh" in withFreshDb {
      implicit lc =>
        for {
          res1 <- fromStartupMode(dao, CreateAndStart)
          _ = res1 shouldBe true
          versions <- dao.transact(
            sql"SELECT version FROM ${queries.jsonApiSchemaVersionTableName}"
              .query[Int]
              .nel
          )
        } yield {
          Set.from(versions.toList) shouldBe Set(queries.schemaVersion)
        }
    }

  }

  "No collisions appear when creating tables with prefix when prior tables have been created without prefix" in {
    def removeAndInit(dao: ContractDao)(implicit logHandler: LogHandler = dao.logHandler) = {
      import dao.jdbcDriver.q.queries
      dao.transact(queries.dropAllTablesIfExist.flatMap(_ => queries.initDatabase))
    }
    for {
      _ <- removeAndInit(daoWithoutPrefix)
      _ <- removeAndInit(dao)
    } yield succeed
  }.unsafeToFuture()

}
