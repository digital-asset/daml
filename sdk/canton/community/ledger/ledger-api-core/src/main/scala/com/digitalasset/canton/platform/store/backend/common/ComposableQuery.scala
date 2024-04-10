// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.{ParameterValue, Row, SimpleSql, ToParameterValue}

import scala.collection.mutable

object ComposableQuery {

  sealed trait QueryPart
  object QueryPart {
    import scala.language.implicitConversions
    implicit def from[A](a: A)(implicit c: ToParameterValue[A]): SingleParameter =
      SingleParameter(c(a))
  }

  final case class SingleParameter(parameterValue: ParameterValue) extends QueryPart
  final case class CompositeSql(stringParts: Seq[String], valueParts: Seq[QueryPart])
      extends QueryPart {
    assert(
      stringParts.size - 1 == valueParts.size,
      s"contract of CompositePart violated: the size of StringParts must be one bigger than the size of valueParts",
    )
  }

  implicit class SqlStringInterpolation(val sc: StringContext) extends AnyVal {
    def SQL(args: QueryPart*): SimpleSql[Row] = {
      val (stringParts, valueParts) = flattenComposite(sc.parts, args)

      anorm
        .SqlStringInterpolation(StringContext(stringParts*))
        .SQL(valueParts*)
    }

    def cSQL(args: QueryPart*): CompositeSql = CompositeSql(sc.parts, args)
  }

  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  private[common] def flattenComposite(
      stringContextParts: Iterable[String],
      values: Iterable[QueryPart],
  ): (Seq[String], Seq[ParameterValue]) = {
    val stringParts = mutable.ArrayBuffer.empty[String]
    val valueParts = mutable.ArrayBuffer.empty[ParameterValue]
    // need to maintain StringContext contract: string parts always have size 1 bigger than values
    def addStringPart(stringPart: String): Unit =
      if (stringParts.size > valueParts.size) {
        stringParts.update(stringParts.size - 1, stringParts.last + stringPart)
      } else
        stringParts += stringPart
    def go(
        stringPartsIterator: Iterator[String],
        valuePartsIterator: Iterator[QueryPart],
    ): Unit = {
      stringPartsIterator.zip(valuePartsIterator).foreach {
        case (prefix, SingleParameter(parameterValue)) =>
          addStringPart(prefix)
          valueParts += parameterValue

        case (prefix, CompositeSql(strings, values)) =>
          addStringPart(prefix)
          go(strings.iterator, values.iterator)
      }
      stringPartsIterator.foreach(addStringPart)
    }

    go(stringContextParts.iterator, values.iterator)

    (stringParts.toSeq, valueParts.toSeq)
  }

  implicit class CompositConcatenationOps(val composits: Iterable[CompositeSql]) extends AnyVal {
    def mkComposite(start: String, sep: String, end: String): CompositeSql = {
      require(composits.nonEmpty, "composits must be non-empty")
      CompositeSql(
        stringParts = start :: List.fill(composits.size - 1)(sep) ::: List(end),
        valueParts = composits.toSeq,
      )
    }
  }
}
