// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.problems

import java.time.Instant

import com.digitalasset.ledger.client.binding.encoding._
import com.digitalasset.ledger.client.binding.{Primitive => P}
import com.digitalasset.sample.{MyMain => M}
import com.digitalasset.slick.PostgreSqlConnectionWithContainer
import com.digitalasset.slick.SlickUtil.{createTable, dropTable}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpec, BeforeAndAfterAll, Succeeded}
import slick.jdbc.H2Profile.api._
import slick.lifted.{TableQuery, Tag}

import scala.concurrent.Future
import scala.util.{Failure, Success}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class PostgreSqlKnownProblemSpec extends AsyncWordSpec with ScalaFutures with BeforeAndAfterAll {

  val con = new PostgreSqlConnectionWithContainer()
  val profile = con.profile
  val db = con.db

  val aProblemString = "a unicode string that causes PostgreSQL exception: \u0000"
  val expectedErrorMessage = """invalid byte sequence for encoding "UTF8": 0x00"""

  override protected def afterAll(): Unit = {
    con.close()
  }

  "Problem dataset scenarios" should {
    "PostgreSQL cannot handle a unicode string with \\u0000 character" in {
      val contract = M.TemplateWithEmptyRecords(
        party = P.Party("Alice"),
        emptyRecord = M.EmptyRecord(),
        emptyRecordWithTParam = M.EmptyRecordWithTParam(),
        polyRec = M.PolyRec(
          a = 123L,
          b = aProblemString,
          c = ts("2000-01-01T00:00:00.0Z")
        )
      )

      val enc = SlickTypeEncoding(profile)
      val encOut = LfEncodable.encoding[M.TemplateWithEmptyRecords](enc)
      val tableFn = enc.table(encOut)._2: Tag => profile.Table[M.TemplateWithEmptyRecords]
      val table = TableQuery[profile.Table[M.TemplateWithEmptyRecords]](tableFn)

      val insert = table ++= Seq(contract)
      val futureResult = for {
        _ <- createTable(profile)(db, table)
        c1 <- db.run(insert)
        r1 <- db.run(table.result): Future[Seq[M.TemplateWithEmptyRecords]]
        _ <- dropTable(profile)(db, table)
      } yield r1

      futureResult.transform {
        case Failure(e: java.sql.BatchUpdateException) =>
          if (e.getMessage.contains(expectedErrorMessage))
            Success(Succeeded)
          else fail(s"unexpected error message: ${e.getMessage}")
        case a @ _ =>
          fail(s"Expecting java.sql.BatchUpdateException, but got: $a")
      }
    }
  }

  private def ts(s: String): P.Timestamp =
    P.Timestamp
      .discardNanos(Instant.parse(s))
      .getOrElse(sys.error("expected `P.Timestamp` friendly `Instant`"))
}
