// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import cats.effect.IO
import com.daml.http.dbbackend.{ContractDao, SupportedJdbcDriver}
import com.daml.http.util.Logging.{InstanceUUID, instanceUUIDLogCtx}
import com.daml.logging.LoggingContextOf
import com.daml.scalautil.Statement.discard
import com.daml.testing.postgresql.PostgresAroundAll
import doobie.util.log
import org.scalatest.Assertion
import spray.json.{JsString, JsValue}
import scala.collection.compat._

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class HttpServiceWithPostgresIntTest
    extends AbstractHttpServiceIntegrationTest
    with PostgresAroundAll
    with HttpServicePostgresInt {

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None

  "DbStartupOps" - {
    import doobie.implicits.toSqlInterpolator, DbStartupOps.DbVersionState._, DbStartupOps._,
    DbStartupMode._

    implicit lazy val _dao: ContractDao = dao
    implicit lazy val logHandler: log.LogHandler = dao.logHandler
    implicit lazy val jdbcDriver: SupportedJdbcDriver = dao.jdbcDriver

    def withFreshDb(fun: LoggingContextOf[InstanceUUID] => IO[Assertion]): Future[Assertion] =
      dao
        .transact(jdbcDriver.queries.dropAllTablesIfExist)
        .flatMap(_ => fun(instanceUUIDLogCtx()))
        .unsafeToFuture()

    "fromStartupMode called with CreateIfNeededAndStart will re-initialize the cache when no schema version is available" in withFreshDb {
      implicit lc =>
        for {
          res1 <- fromStartupMode(dao, CreateIfNeededAndStart)
          _ = res1 shouldBe true
          version <- dao.transact(
            sql"SELECT version FROM json_api_schema_version".query[Int].unique
          )
        } yield version shouldBe jdbcDriver.queries.schemaVersion
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
          _ <- dao.transact(sql"DELETE FROM json_api_schema_version".update.run)
          _ <- dao.transact(
            sql"INSERT INTO json_api_schema_version(version) VALUES($wrongVersion)".update.run
          )
          res2 <- getDbVersionState
        } yield res2 shouldBe Right(Mismatch(jdbcDriver.queries.schemaVersion, wrongVersion))
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
          versions <- dao.transact(sql"SELECT version FROM json_api_schema_version".query[Int].nel)
        } yield {
          Set.from(versions.toList) shouldBe Set(jdbcDriver.queries.schemaVersion)
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
          _ <- dao.transact(sql"DELETE FROM json_api_schema_version".update.run)
          _ <- dao.transact(sql"INSERT INTO json_api_schema_version(version) VALUES(-1)".update.run)
          res2 <- fromStartupMode(dao, StartOnly)
        } yield res2 shouldBe false
    }

    "fromStartupMode called with CreateOnly initializes the schema correctly when the db is fresh" in withFreshDb {
      implicit lc =>
        for {
          res1 <- fromStartupMode(dao, CreateOnly)
          _ = res1 shouldBe true
          versions <- dao.transact(sql"SELECT version FROM json_api_schema_version".query[Int].nel)
        } yield {
          Set.from(versions.toList) shouldBe Set(jdbcDriver.queries.schemaVersion)
        }
    }

    "fromStartupMode called with CreateAndStart initializes the schema correctly when the db is fresh" in withFreshDb {
      implicit lc =>
        for {
          res1 <- fromStartupMode(dao, CreateAndStart)
          _ = res1 shouldBe true
          versions <- dao.transact(sql"SELECT version FROM json_api_schema_version".query[Int].nel)
        } yield {
          Set.from(versions.toList) shouldBe Set(jdbcDriver.queries.schemaVersion)
        }
    }

  }

  "query persists all active contracts" in withHttpService { (uri, encoder, _) =>
    searchExpectOk(
      searchDataSet,
      jsObject("""{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR"}}"""),
      uri,
      encoder,
    ).flatMap { searchResult: List[domain.ActiveContract[JsValue]] =>
      discard { searchResult should have size 2 }
      discard { searchResult.map(getField("currency")) shouldBe List.fill(2)(JsString("EUR")) }
      selectAllDbContracts.flatMap { listFromDb =>
        discard { listFromDb should have size searchDataSet.size.toLong }
        val actualCurrencyValues: List[String] = listFromDb
          .flatMap { case (_, _, _, payload, _, _, _) =>
            payload.asJsObject().getFields("currency")
          }
          .collect { case JsString(a) => a }
        val expectedCurrencyValues = List("EUR", "EUR", "GBP", "BTC")
        // the initial create commands submitted asynchronously, we don't know the exact order, that is why sorted
        actualCurrencyValues.sorted shouldBe expectedCurrencyValues.sorted
      }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def selectAllDbContracts
      : Future[List[(String, String, JsValue, JsValue, Vector[String], Vector[String], String)]] = {
    import doobie.implicits._, doobie.postgres.implicits._
    import dao.jdbcDriver._, queries.Implicits._

    val q =
      sql"""SELECT contract_id, tpid, key, payload, signatories, observers, agreement_text FROM contract"""
        .query[(String, String, JsValue, JsValue, Vector[String], Vector[String], String)]

    dao.transact(q.to[List]).unsafeToFuture()
  }

  private def getField(k: String)(a: domain.ActiveContract[JsValue]): JsValue =
    a.payload.asJsObject().getFields(k) match {
      case Seq(x) => x
      case xs @ _ => fail(s"Expected exactly one value, got: $xs")
    }
}
