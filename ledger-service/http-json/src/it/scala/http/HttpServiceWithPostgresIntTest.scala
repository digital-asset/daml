// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.scalautil.Statement.discard
import com.daml.testing.postgresql.PostgresAroundAll
import spray.json.{JsString, JsValue}

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class HttpServiceWithPostgresIntTest
    extends QueryStoreAndAuthDependentIntegrationTest
    with PostgresAroundAll
    with HttpServicePostgresInt {

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None

  "query persists all active contracts" in withHttpService { fixture =>
    fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (party, headers) =>
      val searchDataSet = genSearchDataSet(party)
      searchExpectOk(
        searchDataSet,
        jsObject("""{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR"}}"""),
        fixture,
        headers,
      ).flatMap { searchResult =>
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
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def selectAllDbContracts
      : Future[List[(String, String, JsValue, JsValue, Vector[String], Vector[String], String)]] = {
    import doobie.implicits._, doobie.postgres.implicits._
    import dao.jdbcDriver.q.queries, queries.Implicits._

    val q =
      sql"""SELECT contract_id, tpid, key, payload, signatories, observers, agreement_text FROM ${queries.contractTableName}"""
        .query[(String, String, JsValue, JsValue, Vector[String], Vector[String], String)]

    dao.transact(q.to[List]).unsafeToFuture()
  }

  private def getField(k: String)(a: domain.ActiveContract[Any, JsValue]): JsValue =
    a.payload.asJsObject().getFields(k) match {
      case Seq(x) => x
      case xs @ _ => fail(s"Expected exactly one value, got: $xs")
    }
}

final class HttpServiceWithPostgresIntTestCustomToken
    extends HttpServiceWithPostgresIntTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken
