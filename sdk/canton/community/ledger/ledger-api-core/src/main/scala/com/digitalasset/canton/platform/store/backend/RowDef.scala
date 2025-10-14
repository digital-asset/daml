// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import anorm.{Row, RowParser, SimpleSql, SqlRequestError, ~}
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
    sql(cSQL"#${columns.mkString(", ")}").asVectorOf(rowParser)(connection)

  def map[U](f: T => U): RowDef[U] = RowDef(columns, rowParser.map(f))

  def branch[U, X >: T](branches: (X, RowDef[U])*): RowDef[U] = {
    val branchMap = branches.toMap
    RowDef(
      columns = branches.flatMap(_._2.columns).++(columns).distinct.toVector,
      rowParser = rowParser.flatMap { branchValue =>
        branchMap.get(branchValue) match {
          case Some(rowDef) => rowDef.rowParser
          case None =>
            _ =>
              anorm.Error(
                SqlRequestError(
                  new IllegalStateException(
                    s"Cannot find suitable branch for result parsing for extracted branch value $branchValue"
                  )
                )
              )
        }
      },
    )
  }
}

object RowDef {
  def static[T](t: T): RowDef[T] =
    RowDef(Vector.empty, _ => anorm.Success(t))

  def column[T](
      columnName: String,
      rowParser: String => RowParser[T],
  ): RowDef[T] =
    RowDef(Vector(columnName), rowParser(columnName))

  def combine[A, B, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
  )(f: (A, B) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2).flatMap(_.columns).distinct,
      (p1.rowParser ~ p2.rowParser).map { case r1 ~ r2 => f(r1, r2) },
    )

  def combine[A, B, C, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
  )(f: (A, B, C) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3).flatMap(_.columns).distinct,
      (p1.rowParser ~ p2.rowParser ~ p3.rowParser)
        map { case r1 ~ r2 ~ r3 => f(r1, r2, r3) },
    )

  def combine[A, B, C, D, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
  )(f: (A, B, C, D) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4).flatMap(_.columns).distinct,
      (p1.rowParser ~ p2.rowParser ~ p3.rowParser ~ p4.rowParser)
        map { case r1 ~ r2 ~ r3 ~ r4 => f(r1, r2, r3, r4) },
    )

  def combine[A, B, C, D, E, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
      p5: RowDef[E],
  )(f: (A, B, C, D, E) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4, p5).flatMap(_.columns).distinct,
      (p1.rowParser ~ p2.rowParser ~ p3.rowParser ~ p4.rowParser ~ p5.rowParser)
        map { case r1 ~ r2 ~ r3 ~ r4 ~ r5 => f(r1, r2, r3, r4, r5) },
    )

  def combine[A, B, C, D, E, F, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
      p5: RowDef[E],
      p6: RowDef[F],
  )(f: (A, B, C, D, E, F) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4, p5, p6).flatMap(_.columns).distinct,
      (p1.rowParser ~ p2.rowParser ~ p3.rowParser ~ p4.rowParser ~ p5.rowParser ~ p6.rowParser)
        map { case r1 ~ r2 ~ r3 ~ r4 ~ r5 ~ r6 => f(r1, r2, r3, r4, r5, r6) },
    )

  def combine[A, B, C, D, E, F, G, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
      p5: RowDef[E],
      p6: RowDef[F],
      p7: RowDef[G],
  )(f: (A, B, C, D, E, F, G) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4, p5, p6, p7).flatMap(_.columns).distinct,
      (p1.rowParser ~ p2.rowParser ~ p3.rowParser ~ p4.rowParser ~ p5.rowParser ~ p6.rowParser ~ p7.rowParser)
        map { case r1 ~ r2 ~ r3 ~ r4 ~ r5 ~ r6 ~ r7 => f(r1, r2, r3, r4, r5, r6, r7) },
    )

  def combine[A, B, C, D, E, F, G, H, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
      p5: RowDef[E],
      p6: RowDef[F],
      p7: RowDef[G],
      p8: RowDef[H],
  )(f: (A, B, C, D, E, F, G, H) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4, p5, p6, p7, p8).flatMap(_.columns).distinct,
      (p1.rowParser ~ p2.rowParser ~ p3.rowParser ~ p4.rowParser ~ p5.rowParser ~ p6.rowParser ~ p7.rowParser ~ p8.rowParser)
        map { case r1 ~ r2 ~ r3 ~ r4 ~ r5 ~ r6 ~ r7 ~ r8 => f(r1, r2, r3, r4, r5, r6, r7, r8) },
    )

  def combine[A, B, C, D, E, F, G, H, I, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
      p5: RowDef[E],
      p6: RowDef[F],
      p7: RowDef[G],
      p8: RowDef[H],
      p9: RowDef[I],
  )(f: (A, B, C, D, E, F, G, H, I) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4, p5, p6, p7, p8, p9)
        .flatMap(_.columns)
        .distinct,
      (
        p1.rowParser ~
          p2.rowParser ~
          p3.rowParser ~
          p4.rowParser ~
          p5.rowParser ~
          p6.rowParser ~
          p7.rowParser ~
          p8.rowParser ~
          p9.rowParser
      ) map {
        case r1 ~
            r2 ~
            r3 ~
            r4 ~
            r5 ~
            r6 ~
            r7 ~
            r8 ~
            r9 =>
          f(
            r1,
            r2,
            r3,
            r4,
            r5,
            r6,
            r7,
            r8,
            r9,
          )
      },
    )

  def combine[A, B, C, D, E, F, G, H, I, J, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
      p5: RowDef[E],
      p6: RowDef[F],
      p7: RowDef[G],
      p8: RowDef[H],
      p9: RowDef[I],
      p10: RowDef[J],
  )(f: (A, B, C, D, E, F, G, H, I, J) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10)
        .flatMap(_.columns)
        .distinct,
      (
        p1.rowParser ~
          p2.rowParser ~
          p3.rowParser ~
          p4.rowParser ~
          p5.rowParser ~
          p6.rowParser ~
          p7.rowParser ~
          p8.rowParser ~
          p9.rowParser ~
          p10.rowParser
      ) map {
        case r1 ~
            r2 ~
            r3 ~
            r4 ~
            r5 ~
            r6 ~
            r7 ~
            r8 ~
            r9 ~
            r10 =>
          f(
            r1,
            r2,
            r3,
            r4,
            r5,
            r6,
            r7,
            r8,
            r9,
            r10,
          )
      },
    )

  def combine[A, B, C, D, E, F, G, H, I, J, K, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
      p5: RowDef[E],
      p6: RowDef[F],
      p7: RowDef[G],
      p8: RowDef[H],
      p9: RowDef[I],
      p10: RowDef[J],
      p11: RowDef[K],
  )(f: (A, B, C, D, E, F, G, H, I, J, K) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11)
        .flatMap(_.columns)
        .distinct,
      (
        p1.rowParser ~
          p2.rowParser ~
          p3.rowParser ~
          p4.rowParser ~
          p5.rowParser ~
          p6.rowParser ~
          p7.rowParser ~
          p8.rowParser ~
          p9.rowParser ~
          p10.rowParser ~
          p11.rowParser
      ) map {
        case r1 ~
            r2 ~
            r3 ~
            r4 ~
            r5 ~
            r6 ~
            r7 ~
            r8 ~
            r9 ~
            r10 ~
            r11 =>
          f(
            r1,
            r2,
            r3,
            r4,
            r5,
            r6,
            r7,
            r8,
            r9,
            r10,
            r11,
          )
      },
    )

  def combine[A, B, C, D, E, F, G, H, I, J, K, L, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
      p5: RowDef[E],
      p6: RowDef[F],
      p7: RowDef[G],
      p8: RowDef[H],
      p9: RowDef[I],
      p10: RowDef[J],
      p11: RowDef[K],
      p12: RowDef[L],
  )(f: (A, B, C, D, E, F, G, H, I, J, K, L) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12)
        .flatMap(_.columns)
        .distinct,
      (
        p1.rowParser ~
          p2.rowParser ~
          p3.rowParser ~
          p4.rowParser ~
          p5.rowParser ~
          p6.rowParser ~
          p7.rowParser ~
          p8.rowParser ~
          p9.rowParser ~
          p10.rowParser ~
          p11.rowParser ~
          p12.rowParser
      ) map {
        case r1 ~
            r2 ~
            r3 ~
            r4 ~
            r5 ~
            r6 ~
            r7 ~
            r8 ~
            r9 ~
            r10 ~
            r11 ~
            r12 =>
          f(
            r1,
            r2,
            r3,
            r4,
            r5,
            r6,
            r7,
            r8,
            r9,
            r10,
            r11,
            r12,
          )
      },
    )

  def combine[A, B, C, D, E, F, G, H, I, J, K, L, M, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
      p5: RowDef[E],
      p6: RowDef[F],
      p7: RowDef[G],
      p8: RowDef[H],
      p9: RowDef[I],
      p10: RowDef[J],
      p11: RowDef[K],
      p12: RowDef[L],
      p13: RowDef[M],
  )(f: (A, B, C, D, E, F, G, H, I, J, K, L, M) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13)
        .flatMap(_.columns)
        .distinct,
      (
        p1.rowParser ~
          p2.rowParser ~
          p3.rowParser ~
          p4.rowParser ~
          p5.rowParser ~
          p6.rowParser ~
          p7.rowParser ~
          p8.rowParser ~
          p9.rowParser ~
          p10.rowParser ~
          p11.rowParser ~
          p12.rowParser ~
          p13.rowParser
      ) map {
        case r1 ~
            r2 ~
            r3 ~
            r4 ~
            r5 ~
            r6 ~
            r7 ~
            r8 ~
            r9 ~
            r10 ~
            r11 ~
            r12 ~
            r13 =>
          f(
            r1,
            r2,
            r3,
            r4,
            r5,
            r6,
            r7,
            r8,
            r9,
            r10,
            r11,
            r12,
            r13,
          )
      },
    )

  def combine[A, B, C, D, E, F, G, H, I, J, K, L, M, N, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
      p5: RowDef[E],
      p6: RowDef[F],
      p7: RowDef[G],
      p8: RowDef[H],
      p9: RowDef[I],
      p10: RowDef[J],
      p11: RowDef[K],
      p12: RowDef[L],
      p13: RowDef[M],
      p14: RowDef[N],
  )(f: (A, B, C, D, E, F, G, H, I, J, K, L, M, N) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14)
        .flatMap(_.columns)
        .distinct,
      (
        p1.rowParser ~
          p2.rowParser ~
          p3.rowParser ~
          p4.rowParser ~
          p5.rowParser ~
          p6.rowParser ~
          p7.rowParser ~
          p8.rowParser ~
          p9.rowParser ~
          p10.rowParser ~
          p11.rowParser ~
          p12.rowParser ~
          p13.rowParser ~
          p14.rowParser
      ) map {
        case r1 ~
            r2 ~
            r3 ~
            r4 ~
            r5 ~
            r6 ~
            r7 ~
            r8 ~
            r9 ~
            r10 ~
            r11 ~
            r12 ~
            r13 ~
            r14 =>
          f(
            r1,
            r2,
            r3,
            r4,
            r5,
            r6,
            r7,
            r8,
            r9,
            r10,
            r11,
            r12,
            r13,
            r14,
          )
      },
    )

  def combine[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
      p5: RowDef[E],
      p6: RowDef[F],
      p7: RowDef[G],
      p8: RowDef[H],
      p9: RowDef[I],
      p10: RowDef[J],
      p11: RowDef[K],
      p12: RowDef[L],
      p13: RowDef[M],
      p14: RowDef[N],
      p15: RowDef[O],
  )(f: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15)
        .flatMap(_.columns)
        .distinct,
      (
        p1.rowParser ~
          p2.rowParser ~
          p3.rowParser ~
          p4.rowParser ~
          p5.rowParser ~
          p6.rowParser ~
          p7.rowParser ~
          p8.rowParser ~
          p9.rowParser ~
          p10.rowParser ~
          p11.rowParser ~
          p12.rowParser ~
          p13.rowParser ~
          p14.rowParser ~
          p15.rowParser
      ) map {
        case r1 ~
            r2 ~
            r3 ~
            r4 ~
            r5 ~
            r6 ~
            r7 ~
            r8 ~
            r9 ~
            r10 ~
            r11 ~
            r12 ~
            r13 ~
            r14 ~
            r15 =>
          f(
            r1,
            r2,
            r3,
            r4,
            r5,
            r6,
            r7,
            r8,
            r9,
            r10,
            r11,
            r12,
            r13,
            r14,
            r15,
          )
      },
    )

  def combine[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, RESULT](
      p1: RowDef[A],
      p2: RowDef[B],
      p3: RowDef[C],
      p4: RowDef[D],
      p5: RowDef[E],
      p6: RowDef[F],
      p7: RowDef[G],
      p8: RowDef[H],
      p9: RowDef[I],
      p10: RowDef[J],
      p11: RowDef[K],
      p12: RowDef[L],
      p13: RowDef[M],
      p14: RowDef[N],
      p15: RowDef[O],
      p16: RowDef[P],
  )(f: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) => RESULT): RowDef[RESULT] =
    RowDef(
      Vector(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16)
        .flatMap(_.columns)
        .distinct,
      (
        p1.rowParser ~
          p2.rowParser ~
          p3.rowParser ~
          p4.rowParser ~
          p5.rowParser ~
          p6.rowParser ~
          p7.rowParser ~
          p8.rowParser ~
          p9.rowParser ~
          p10.rowParser ~
          p11.rowParser ~
          p12.rowParser ~
          p13.rowParser ~
          p14.rowParser ~
          p15.rowParser ~
          p16.rowParser
      ) map {
        case r1 ~
            r2 ~
            r3 ~
            r4 ~
            r5 ~
            r6 ~
            r7 ~
            r8 ~
            r9 ~
            r10 ~
            r11 ~
            r12 ~
            r13 ~
            r14 ~
            r15 ~
            r16 =>
          f(
            r1,
            r2,
            r3,
            r4,
            r5,
            r6,
            r7,
            r8,
            r9,
            r10,
            r11,
            r12,
            r13,
            r14,
            r15,
            r16,
          )
      },
    )
}
