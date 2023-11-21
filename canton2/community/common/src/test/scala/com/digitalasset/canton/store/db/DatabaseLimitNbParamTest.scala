// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.util.retry.RetryUtil.{DbExceptionRetryable, FatalErrorKind}
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import org.scalatest.BeforeAndAfterAll
import slick.sql.SqlAction

import scala.concurrent.Future

import DbStorage.Implicits.BuilderChain.*

trait DatabaseLimitNbParamTest
    extends BaseTestWordSpec
    with BeforeAndAfterAll
    with HasExecutionContext {
  this: DbTest =>

  lazy val rawStorage: DbStorage = storage.underlying
  import rawStorage.api.*

  def createTableAction: SqlAction[Int, NoStream, Effect.Write]

  override def beforeAll(): Unit = {
    super.beforeAll()

    rawStorage
      .queryAndUpdate(
        DBIO.seq(
          sqlu"drop table database_limit_nb_param_test".asTry, // Try to drop, in case it already exists.
          createTableAction,
        ),
        functionFullName,
      )
      .futureValue
  }

  override def cleanDb(storage: DbStorage): Future[Unit] =
    rawStorage.update_(
      sqlu"truncate table database_limit_nb_param_test",
      functionFullName,
    )

  def nbOfParametersLimit: Int

  def insertCommand(nbVal: Int): DbStorage.SQLActionBuilderChain

  "The storage" when {
    "exceeding DB limit on the number of parameters in a statement" should {
      "fail without retrying" in {
        // Our test query uses 5 parameters per key in the prepared statement
        val nbKeys = nbOfParametersLimit / 5 + 1

        val query = insertCommand(nbKeys)

        rawStorage
          .update(query.asUpdate, "parameter limit query", maxRetries = 1)
          .transformWith { outcome =>
            val errorKind = DbExceptionRetryable.retryOK(outcome, logger)

            errorKind match {
              case FatalErrorKind =>
              case _ => fail("Database error kind should be fatal")
            }

            Future.successful(true)
          }
          .futureValue shouldBe true
      }
    }
  }

}

class DatabaseLimitNbParamTestPostgres extends DatabaseLimitNbParamTest with PostgresTest {
  import rawStorage.api.*

  override lazy val createTableAction: SqlAction[Int, NoStream, Effect.Write] =
    sqlu"""create table database_limit_nb_param_test(
             key bigint primary key,
             val1 bigint not null,
             val2 bigint not null,
             val3 bigint not null,
             val4 bigint not null)"""

  override val nbOfParametersLimit: Int = (1 << 16) - 1 // As of JDBC driver v42.4.0

  override def insertCommand(nbVal: Int): DbStorage.SQLActionBuilderChain = {
    val values =
      (1 to nbVal).map(key => sql"""($key, $key, $key, $key, $key)""").intercalate(sql", ")

    sql"""insert into database_limit_nb_param_test(key, val1, val2, val3, val4)
             values """ ++ values ++
      sql""" on conflict (key) do nothing"""
  }
}
