// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.{Cursor, Row, RowParser, SimpleSql}

import java.sql.Connection
import scala.util.{Failure, Success, Try}

private[backend] object SimpleSqlExtensions {

  implicit final class `SimpleSql ops`(val sql: SimpleSql[Row]) extends AnyVal {

    /** Returns the result of [[sql]] as a [[Vector]].
      *
      * Allows to avoid linear operations in lists when using the default
      * [[anorm.ResultSetParser]]s (e.g. when retrieving the result set
      * length in [[com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream]]
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
        .fold(
          _.headOption.fold(throw new NoSuchElementException("empty list of errors"))(throw _),
          _.fold(throw _, identity),
        )
    }

    def asSingle[A](parser: RowParser[A])(implicit connection: Connection): A =
      sql.as(parser.single)

    def asSingleOpt[A](parser: RowParser[A])(implicit connection: Connection): Option[A] =
      sql.as(parser.singleOpt)
  }

}
