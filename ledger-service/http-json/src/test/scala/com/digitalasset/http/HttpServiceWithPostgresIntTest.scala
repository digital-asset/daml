// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll
import spray.json.{JsString, JsValue}
import com.digitalasset.http.Statement.discard

import scala.concurrent.Future

class HttpServiceWithPostgresIntTest
    extends AbstractHttpServiceIntegrationTest
    with PostgresAroundAll {

  override def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)

  override def staticContentConfig: Option[StaticContentConfig] = None

  // has to be lazy because postgresFixture is NOT initialized yet
  private lazy val jdbcConfig_ = JdbcConfig(
    driver = "org.postgresql.Driver",
    url = postgresFixture.jdbcUrl,
    user = "test",
    password = "",
    createSchema = true)

  private lazy val dao = dbbackend.ContractDao(
    jdbcDriver = jdbcConfig_.driver,
    jdbcUrl = jdbcConfig_.url,
    username = jdbcConfig_.user,
    password = jdbcConfig_.password
  )

  "contracts/search persists all active contracts" in withHttpService { (uri, encoder, _) =>
    searchWithQuery(
      searchDataSet,
      jsObject(
        """{"%templates": [{"moduleName": "Iou", "entityName": "Iou"}], "currency": "EUR"}"""),
      uri,
      encoder
    ).flatMap { searchResult: List[domain.ActiveContract[JsValue]] =>
      discard { searchResult should have size 2 }
      discard { searchResult.map(getField("currency")) shouldBe List.fill(2)(JsString("EUR")) }
      selectAllDbContracts.flatMap { listFromDb =>
        discard { listFromDb should have size searchDataSet.size.toLong }
        val actualCurrencyValues: List[String] = listFromDb
          .flatMap { case (_, args, _) => args.asJsObject().getFields("currency") }
          .collect { case JsString(a) => a }
        val expectedCurrencyValues = List("EUR", "EUR", "GBP", "BTC")
        // the initial create commands submitted asynchronously, we don't know the exact order, that is why sorted
        actualCurrencyValues.sorted shouldBe expectedCurrencyValues.sorted
      }
    }
  }

  private def selectAllDbContracts: Future[List[(String, JsValue, Vector[String])]] = {
    import doobie.implicits._, doobie.postgres.implicits._
    import com.digitalasset.http.dbbackend.Queries.Implicits._
    import dao.logHandler

    val q =
      sql"""SELECT contract_id, create_arguments, witness_parties FROM contract"""
        .query[(String, JsValue, Vector[String])]

    dao.transact(q.to[List]).unsafeToFuture()
  }

  private def getField(k: String)(a: domain.ActiveContract[JsValue]): JsValue =
    a.argument.asJsObject().getFields(k) match {
      case Seq(x) => x
      case xs @ _ => fail(s"Expected exactly one value, got: $xs")
    }
}
