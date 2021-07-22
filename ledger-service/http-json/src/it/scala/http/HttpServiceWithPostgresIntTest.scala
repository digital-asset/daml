// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.dbbackend.{ContractDao, SupportedJdbcDriver}
import com.daml.scalautil.Statement.discard
import com.daml.testing.postgresql.PostgresAroundAll
import doobie.util.log
import spray.json.{JsString, JsValue}

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class HttpServiceWithPostgresIntTest
    extends AbstractHttpServiceIntegrationTest
    with PostgresAroundAll
    with HttpServicePostgresInt {

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None

  // has to be lazy because jdbcConfig_ is NOT initialized yet
  private lazy val dao = dbbackend.ContractDao(
    jdbcDriver = jdbcConfig_.driver,
    jdbcUrl = jdbcConfig_.url,
    username = jdbcConfig_.user,
    password = jdbcConfig_.password,
  )

  implicit lazy val logHandler: log.LogHandler = dao.logHandler
  implicit lazy val jdbcDriver: SupportedJdbcDriver = dao.jdbcDriver

  "if the schema version in the db is not equal to the current one the cache will be re-initialized" in {
    import doobie.implicits.toSqlInterpolator
    val res =
      for {
        _ <- dao.transact(ContractDao.initialize(checkIfExists = false))
        _ <- dao.transact(sql"DELETE FROM json_api_schema_version".update.run)
        _ <- dao.transact(sql"INSERT INTO json_api_schema_version(version) VALUES(0)".update.run)
        _ <- dao.transact(ContractDao.initialize(checkIfExists = true))
        version <- dao.transact(sql"SELECT version FROM json_api_schema_version".query[Int].unique)
      } yield {
        version should not be 0
        version shouldBe jdbcDriver.queries.schemaVersion
      }
    res.unsafeToFuture()
  }

  "if the schema version in the db is equal to the current one the cache won't be re-initialized" in {
    import doobie.implicits.toSqlInterpolator
    val res =
      for {
        _ <- dao.transact(ContractDao.initialize(checkIfExists = false))
        _ <- dao.transact(sql"INSERT INTO json_api_schema_version(version) VALUES(0)".update.run)
        _ <- dao.transact(ContractDao.initialize(checkIfExists = true))
        versions <- dao.transact(sql"SELECT version FROM json_api_schema_version".query[Int].nel)
      } yield {
        Set.from(versions.toList) should not be Set(jdbcDriver.queries.schemaVersion)
        Set.from(versions.toList) shouldBe Set(0, jdbcDriver.queries.schemaVersion)
      }
    res.unsafeToFuture()
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
