// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store

import java.sql.Connection

import anorm.{Cursor, Row, RowParser, SimpleSql}

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object SimpleSqlAsVectorOf {

  private def throwNonFatal(throwable: Throwable): Nothing =
    throwable match { case NonFatal(exception) => throw exception }

  implicit final class SimpleSqlAsVectorOf(val sql: SimpleSql[Row]) extends AnyVal {

    /**
      * Returns the result of [[sql]] as a [[Vector]].
      *
      * Allows to avoid linear operations in lists when using the default
      * [[anorm.ResultSetParser]]s (e.g. when retrieving the result set
      * length in [[com.digitalasset.platform.store.dao.PaginatingAsyncStream]]
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
        .fold(es => throwNonFatal(es.head), _.fold(throwNonFatal, identity))
    }

  }

}
