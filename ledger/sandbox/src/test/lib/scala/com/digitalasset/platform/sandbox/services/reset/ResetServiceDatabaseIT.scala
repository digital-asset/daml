// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.reset

import java.sql.{Connection, DriverManager}

import anorm.SqlParser._
import anorm.{SQL, SqlStringInterpolation}
import com.daml.ledger.api.testing.utils.MockMessages
import com.daml.platform.sandbox.services.reset.ResetServiceDatabaseIT.countRowsOfAllTables
import com.daml.platform.sandbox.services.{DbInfo, SandboxFixture}
import com.daml.platform.store.DbType
import com.daml.resources.ResourceOwner

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

abstract class ResetServiceDatabaseIT extends ResetServiceITBase with SandboxFixture {

  // Database-backed reset service is allowed a bit more slack
  override def spanScaleFactor: Double = 2.0

  "ResetService" when {

    "run against a database backend" should {

      "leave the tables in the expected state" in {

        val ignored = Set(
          "flyway_schema_history", // this is not touched by resets, it's used for migrations
          "packages" // preserved by the reset to match the compiled packages still loaded in the engine
        )

        for {
          ledgerId <- fetchLedgerId()
          party <- allocateParty(MockMessages.party)
          _ <- submitAndExpectCompletions(ledgerId, 10, party)
          _ <- reset(ledgerId)
          counts <- countRowsOfAllTables(ignored, database.get)
        } yield {

          val expectedToHaveOneItem = Set(
            "parameters" // a new set of parameters is stored at startup
          )

          for ((table, count) <- counts if expectedToHaveOneItem(table)) {
            withClue(s"$table has $count item(s): ") {
              count shouldBe 1
            }
          }

          // FIXME this appears to be racy, forcing us to make a loose check
          val expectedToHaveOneItemOrLess = Set(
            "configuration_entries"
          )

          for ((table, count) <- counts if expectedToHaveOneItemOrLess(table)) {
            withClue(s"$table has $count item(s): ") {
              count should be <= 1
            }
          }

          // Everything else should be empty
          val exceptions = ignored union expectedToHaveOneItem union expectedToHaveOneItemOrLess
          val expectedToBeEmpty = counts.keySet.diff(exceptions)

          for ((table, count) <- counts if expectedToBeEmpty(table)) {
            withClue(s"$table has $count item(s): ") {
              count shouldBe 0
            }
          }

          succeed
        }

      }

    }

  }

}

object ResetServiceDatabaseIT {

  // Very naive helper, supposed to be used exclusively for testing
  private def runQuery[A](dbInfoOwner: ResourceOwner[DbInfo])(sql: DbType => Connection => A)(
      implicit ec: ExecutionContext): Future[A] = {
    val dbTypeAndConnection =
      for {
        dbInfo <- dbInfoOwner
        _ <- ResourceOwner.forTry[Class[_]](() => Try(Class.forName(dbInfo.dbType.driver)))
        connection <- ResourceOwner.forCloseable(() => DriverManager.getConnection(dbInfo.jdbcUrl))
      } yield (dbInfo.dbType, connection)
    dbTypeAndConnection.use {
      case (dbType, connection) => Future.fromTry(Try(sql(dbType)(connection)))
    }
  }

  private def listTables(dbType: DbType)(connection: Connection): List[String] =
    dbType match {
      case DbType.Postgres =>
        SQL"select tablename from pg_catalog.pg_tables where schemaname != 'pg_catalog' and schemaname != 'information_schema'"
          .as(str("tablename").*)(connection)
      case DbType.H2Database =>
        SQL"select table_name from information_schema.tables where table_schema <> 'INFORMATION_SCHEMA'"
          .as(str("table_name").*)(connection)
    }

  private def countRows(tableName: String)(_noDialectDifference: DbType)(
      connection: Connection): Int =
    SQL(s"select count(*) as no_rows from $tableName").as(int("no_rows").single)(connection)

  private def countRowsOfAllTables(ignored: Set[String])(dbType: DbType)(
      connection: Connection): Map[String, Int] =
    listTables(dbType)(connection).collect {
      case table if !ignored(table) => table.toLowerCase -> countRows(table)(dbType)(connection)
    }.toMap

  def countRowsOfAllTables(ignored: Set[String], dbInfoOwner: ResourceOwner[DbInfo])(
      implicit ec: ExecutionContext): Future[Map[String, Int]] =
    runQuery(dbInfoOwner)(countRowsOfAllTables(ignored))

}
