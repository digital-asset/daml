// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.{Cursor, Row, RowParser, SimpleSql}

import scala.util.{Failure, Success, Try}

private[backend] object SimpleSqlAsVectorOf {

  implicit final class `SimpleSql ops`(val sql: SimpleSql[Row]) extends AnyVal {

    /** Returns the result of [[sql]] as a [[Vector]].
      *
      * Allows to avoid linear operations in lists when using the default
      * [[anorm.ResultSetParser]]s (e.g. when retrieving the result set
      * length in [[com.daml.platform.store.dao.PaginatingAsyncStream]]
      *
      * @param parser knows how to turn each row in an [[A]]
      * @param conn an implicit JDBC connection
      * @tparam A the type of each item in the result
      * @throws Throwable if either the query execution or parsing fails
      * @return the query result as a vector
      */
    @throws[Throwable]
    def asVectorOf[A](parser: RowParser[A])(implicit conn: Connection): Vector[A] = {

      @annotation.tailrec
      def go(results: Vector[A])(cursor: Option[Cursor]): Try[Vector[A]] =
        cursor match {
          case Some(cursor) =>
            cursor.row.as(parser) match {
              case Success(value) => go(results :+ value)(cursor.next)
              case Failure(f) => Failure(f)
            }
          case _ => Try(results)
        }

      sql
        .withResult(go(Vector.empty))
        .fold(es => throw es.head, _.fold(throw _, identity))
    }

  }

}
