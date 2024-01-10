// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.resource.DbStorage.Profile.*
import com.digitalasset.canton.util.ErrorUtil
import slick.jdbc.canton.SQLActionBuilder

import scala.concurrent.ExecutionContext

/** Utilities for safely and idempotently inserting records to a datastore. */
object IdempotentInsert {

  /** Execute an insert to an append-only store. If less than `expectedRowsInserted` are found inserted perform
    * a select and verify that the existing data is what we expect to existing in our store.
    * If the existing data fails the provided check predicate a [[java.lang.IllegalStateException]] will be thrown
    * as this indicates either a bug or a configuration error (such as a new node running with a database initialized
    * by another node).
    * If more rows are returned than `expectedRowsInserted` a [[java.lang.IllegalStateException]] is also thrown as
    * this indicates the insert query is doing something contrary to the developers expectations and is likely a bug.
    *
    * To use safely use the `oracleIgnoreIndex` field it **must** be suitable for directly interpolating as a raw string
    * into the query without causing any risk of SQL injection.
    * It is recommended that this value should be hard coded and never come from a value based on the environment
    * or user input.
    *
    * This method will generate the `insert into` prefix for the insert statement, so `insertBuilder` needs to only
    * contain the body of the insert statement excluding the typical `insert into` prefix (this is so for oracle we
    * can generate a suitable [[https://docs.oracle.com/cd/E11882_01/server.112/e41084/sql_elements006.htm#SQLRF30052 IGNORE_ROW_ON_DUPKEY_INDEX]]
    * hint.
    *
    * Typical usage will look like:
    * {{{
    *   insertVerifyingConflicts(
    *     logger,
    *     storage,
    *     "my_table ( pk_col )", // callers MUST ensure this is safe to interpolate directly into the sql query
    *     sql"my_table (pk_col, name) values ($$id, $$name)", // note the missing `insert into` prefix or any `on conflict` postfix. this will be generated appropriately for the target db.
    *     sql"select name from my_table where pk_col = $$id".as[String].head // query values to check successfully exist in the target store.
    *   )(
    *     _ == name,
    *     existingName => s"Expected row $$id to have name $$name but found existing name $$existingName"
    *   )
    * }}}
    *
    * Note that no transaction is started within this method and therefore changes could become visible between the insert
    * and the select that we may then perform. However as the usage of this is intended for append-only stores separate
    * modifications of this data is not expected.
    *
    * Regarding performance the select statement will only be executed if the returned inserted row count is different
    * from the `expectedRowsInserted` value. We anticipate this happening potentially due to crashes or retries due to
    * connectivity issues, and should be rare during normal healthy operation. So typically only the insert should be run.
    */
  def insertVerifyingConflicts[A](
      storage: DbStorage,
      oracleIgnoreIndex: String,
      insertBuilder: => SQLActionBuilder,
      select: DbStorage.DbAction.ReadOnly[A],
      expectedRowsInserted: Int = 1,
  )(existingCondition: A => Boolean, errorMessage: A => String)(implicit
      loggingContext: ErrorLoggingContext,
      executionContext: ExecutionContext,
  ): DbStorage.DbAction.All[Unit] = {
    def assertExisting(): DbStorage.DbAction.ReadOnly[Unit] =
      for {
        existing <- select
      } yield ErrorUtil.requireState(existingCondition(existing), errorMessage(existing))

    for {
      rowsInserted <- insertIgnoringConflicts(storage, oracleIgnoreIndex, insertBuilder)
      _ <-
        if (rowsInserted < expectedRowsInserted)
          assertExisting() // check all of our expected rows exist
        else if (rowsInserted == expectedRowsInserted)
          DbStorage.DbAction.unit // insert ran as expected
        else {
          // we inserted more than we were expecting. this likely suggests our query isn't behaving as we expect and is a bug.
          ErrorUtil.internalError(
            new IllegalStateException(
              s"Command expected to insert at most $expectedRowsInserted but query inserted $rowsInserted"
            )
          )
        }
    } yield ()
  }

  /** Similar to [[insertVerifyingConflicts]] but without verifying that the existing data causing conflicts
    * matches what we expect. Should only be use where the possibilities of conflicts is limited to retries of our insert.
    */
  def insertIgnoringConflicts(
      storage: DbStorage,
      oracleIgnoreIndex: String,
      insertBuilder: => SQLActionBuilder,
  ): DbStorage.DbAction.WriteOnly[Int] = {
    import DbStorage.Implicits.BuilderChain.*
    import storage.api.*

    val insertInto: SQLActionBuilder = storage.profile match {
      case _: Postgres | _: H2 => sql"insert into"
      case _: Oracle => sql"insert /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( #$oracleIgnoreIndex ) */ into"
    }

    val onConflictDoNothing: SQLActionBuilder = storage.profile match {
      case _: Postgres | _: H2 => sql"on conflict do nothing"
      case _: Oracle => sql"" // not supported
    }

    (insertInto ++ sql" " ++ insertBuilder ++ sql" " ++ onConflictDoNothing).asUpdate
  }
}
