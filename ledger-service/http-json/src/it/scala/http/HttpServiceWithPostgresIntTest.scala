// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.scalautil.Statement.discard
import com.daml.testing.postgresql.PostgresAroundAll
import spray.json.{JsString, JsValue}

import scala.concurrent.Future

class HttpServiceWithPostgresIntTest
    extends AbstractHttpServiceIntegrationTest
    with PostgresAroundAll {

  override def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None

  // has to be lazy because postgresFixture is NOT initialized yet
  private lazy val jdbcConfig_ = JdbcConfig(
    driver = "org.postgresql.Driver",
    url = postgresDatabase.url,
    user = "test",
    password = "",
    createSchema = true)

  private lazy val dao = dbbackend.ContractDao(
    jdbcDriver = jdbcConfig_.driver,
    jdbcUrl = jdbcConfig_.url,
    username = jdbcConfig_.user,
    password = jdbcConfig_.password
  )

  "query persists all active contracts" in withHttpService { (uri, encoder, _) =>
    searchExpectOk(
      searchDataSet,
      jsObject("""{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR"}}"""),
      uri,
      encoder
    ).flatMap { searchResult: List[domain.ActiveContract[JsValue]] =>
      discard { searchResult should have size 2 }
      discard { searchResult.map(getField("currency")) shouldBe List.fill(2)(JsString("EUR")) }
      selectAllDbContracts.flatMap { listFromDb =>
        discard { listFromDb should have size searchDataSet.size.toLong }
        val actualCurrencyValues: List[String] = listFromDb
          .flatMap {
            case (_, _, _, payload, _, _, _) => payload.asJsObject().getFields("currency")
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
    import com.daml.http.dbbackend.Queries.Implicits._
    import dao.logHandler
    import doobie.implicits._
    import doobie.postgres.implicits._

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
