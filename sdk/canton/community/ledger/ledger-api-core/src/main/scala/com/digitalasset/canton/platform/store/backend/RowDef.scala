// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import anorm.{Row, RowParser, SimpleSql, ~}
import cats.Applicative
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.*
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*

import java.sql.Connection

final case class RowDef[+T](
    columns: Vector[String],
    rowParser: RowParser[T],
) {
  def queryMultipleRows(sql: CompositeSql => SimpleSql[Row])(implicit
      connection: Connection
  ): Vector[T] =
    sql(columnsCSql).asVectorOf(rowParser)(connection)

  def querySingleOptRow(sql: CompositeSql => SimpleSql[Row])(implicit
      connection: Connection
  ): Option[T] =
    sql(columnsCSql).asSingleOpt(rowParser)(connection)

  private def mapRowParser[U](f: RowParser[T] => RowParser[U]): RowDef[U] =
    RowDef(columns = columns, rowParser = f(rowParser))

  def map[U](f: T => U): RowDef[U] = mapRowParser(_.map(f))

  def ? : RowDef[Option[T]] = mapRowParser(_.?)

  def branch[U, X >: T](branches: (X, RowDef[U])*): RowDef[U] = {
    val branchMap = branches.toMap
    RowDef(
      columns = branches.flatMap(_._2.columns).++(columns).distinct.toVector,
      rowParser = rowParser.flatMap { branchValue =>
        branchMap.get(branchValue) match {
          case Some(rowDef) => rowDef.rowParser
          case None =>
            _ =>
              throw new IllegalStateException(
                s"Cannot find suitable branch for result parsing for extracted branch value $branchValue"
              )
        }
      },
    )
  }

  private val columnsCSql = cSQL"#${columns.mkString(", ")}"
}

object RowDef {
  def static[T](t: T): RowDef[T] =
    RowDef(Vector.empty, _ => anorm.Success(t))

  def column[T](
      columnName: String,
      rowParser: String => RowParser[T],
  ): RowDef[T] =
    RowDef(Vector(columnName), rowParser(columnName))

  implicit val applicative: Applicative[RowDef] =
    new Applicative[RowDef] {
      override def pure[A](x: A): RowDef[A] = static(x)

      override def ap[A, B](ff: RowDef[A => B])(fa: RowDef[A]): RowDef[B] =
        RowDef(
          columns = Vector(ff, fa).flatMap(_.columns).distinct,
          rowParser = (ff.rowParser ~ fa.rowParser).map { case ff ~ fa => ff(fa) },
        )
    }
}
