// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.problems
import java.time.Instant

import com.digitalasset.ledger.client.binding.encoding.{LfEncodable, SlickTypeEncoding}
import com.digitalasset.ledger.client.binding.{Primitive => P}
import com.digitalasset.sample.{MyMain => M}
import com.digitalasset.slick.H2Db
import com.digitalasset.slick.SlickUtil.{createTable, dropTable}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import slick.jdbc.H2Profile.api._
import slick.lifted.{TableQuery, Tag}

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class JavaSqlTimestampSolvedProblemSpec
    extends AsyncWordSpec
    with ScalaFutures
    with Matchers
    with BeforeAndAfterAll {

  private val con = new H2Db()
  private val profile = con.profile
  private val db = con.db

  private val someTimestampsThatUsedToCauseProblems: Set[P.Timestamp] = Set(
    ts("6813-11-03T05:41:04Z"),
    ts("4226-11-05T05:07:48Z"),
    ts("8202-11-07T05:51:35Z"),
    ts("2529-11-06T05:57:36.498937000Z"),
    ts("2529-11-06T05:57:36.498937Z")
  )

  private def ts(s: String): P.Timestamp =
    P.Timestamp
      .discardNanos(Instant.parse(s))
      .getOrElse(sys.error("expected `P.Timestamp` friendly `Instant`"))

  val contract = M.TemplateWithEmptyRecords(
    party = P.Party("Alice"),
    emptyRecord = M.EmptyRecord(),
    emptyRecordWithTParam = M.EmptyRecordWithTParam(),
    polyRec = M.PolyRec(
      a = 123L,
      b = "some string",
      c = ts("2000-01-01T00:00:00.0Z")
    )
  )

  override protected def afterAll(): Unit = {
    con.close()
  }

  "This used to fail running against both H2 and PostgreSQL" in {
    val enc = SlickTypeEncoding(profile)
    val encOut = LfEncodable.encoding[M.TemplateWithEmptyRecords](enc)
    val tableFn = enc.table(encOut)._2: Tag => profile.Table[M.TemplateWithEmptyRecords]
    val table = TableQuery[profile.Table[M.TemplateWithEmptyRecords]](tableFn)

    val cs0: Set[M.TemplateWithEmptyRecords] = someTimestampsThatUsedToCauseProblems.map { t =>
      contract.copy(polyRec = contract.polyRec.copy(c = t))
    }

    for {
      _ <- createTable(profile)(db, table)
      _ <- db.run(table ++= cs0)
      cs1 <- db.run(table.result): Future[Seq[M.TemplateWithEmptyRecords]]
      _ <- dropTable(profile)(db, table)
    } yield {
      cs1.toSet shouldBe cs0
    }
  }
}
