// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll
import spray.json.JsValue
import com.digitalasset.http.Statement.discard
import scala.concurrent.Future

class HttpServiceWithPostgresIntTest
    extends AbstractHttpServiceIntegrationTest
    with PostgresAroundAll {

  override def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)

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
      selectAllDbContracts.flatMap { list =>
        list should have size searchDataSet.size.toLong
      }
    }
  }

  private def selectAllDbContracts: Future[List[(String, JsValue, JsValue)]] = {
    import doobie.implicits._
    import com.digitalasset.http.dbbackend.Queries.Implicits._
    import dao.logHandler

    val q: doobie.Query0[(String, JsValue, JsValue)] =
      sql"""SELECT contract_id, create_arguments, witness_parties FROM contract"""
        .query[(String, JsValue, JsValue)]

    dao.transact(q.to[List]).unsafeToFuture()
  }
}
