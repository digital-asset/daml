// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.PreparedStatement
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

sealed trait PGField[FROM, TO] {
  def fieldName: String
  def extractor: FROM => TO
  def inputFieldName: String = s"${fieldName}_in"
  def selectFieldExpression: String = inputFieldName
  def generateArray(in: Vector[FROM]): Array[_]
  def setDBObject(preparedStatement: PreparedStatement, index: Int, a: Array[_]): Unit =
    preparedStatement.setObject(index + 1, a)
}

final case class PGTimestamp[FROM](fieldName: String, extractor: FROM => Instant)
    extends PGField[FROM, Instant] {
  override def selectFieldExpression: String =
    s"$inputFieldName::timestamp"

  override def generateArray(in: Vector[FROM]): Array[_] =
    in.view
      .map(extractor)
      .map(toPGTimestampString)
      .toArray

  private def toPGTimestampString(instant: Instant): String =
    if (instant == null) null
    else
      instant
        .atZone(ZoneOffset.UTC)
        .toLocalDateTime
        .format(PGTimestamp.PGTimestampFormat)
}

object PGTimestamp {
  private val PGTimestampFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss") // FIXME +micros
}

final case class PGString[FROM](fieldName: String, extractor: FROM => String)
    extends PGField[FROM, String] {
  override def generateArray(in: Vector[FROM]): Array[_] = in.view.map(extractor).toArray
}

final case class PGStringArray[FROM](fieldName: String, extractor: FROM => Iterable[String])
    extends PGField[FROM, Iterable[String]] {
  override def selectFieldExpression: String =
    s"string_to_array($inputFieldName, '|')"

  override def generateArray(in: Vector[FROM]): Array[_] =
    in.view
      .map(extractor)
      .map(encodeTextArray)
      .toArray

  def encodeTextArray(from: Iterable[String]): String =
    if (from == null) null
    else
      from.mkString("|") // FIXME safeguard/escape pipe chars in from
}

final case class PGBytea[FROM](fieldName: String, extractor: FROM => Array[Byte])
    extends PGField[FROM, Array[Byte]] {
  override def generateArray(in: Vector[FROM]): Array[_] = in.view.map(extractor).toArray
}

final case class PGIntOptional[FROM](fieldName: String, extractor: FROM => Option[Int])
    extends PGField[FROM, Option[Int]] {
  override def generateArray(in: Vector[FROM]): Array[_] =
    in.view
      .map(in => extractor(in).map(x => x: java.lang.Integer).orNull)
      .toArray
}

final case class PGBigint[FROM](fieldName: String, extractor: FROM => Long)
    extends PGField[FROM, Long] {
  override def generateArray(in: Vector[FROM]): Array[_] = in.view.map(extractor).toArray
}

final case class PGSmallintOptional[FROM](fieldName: String, extractor: FROM => Option[Int])
    extends PGField[FROM, Option[Int]] {
  override def selectFieldExpression: String =
    s"$inputFieldName::smallint"

  override def generateArray(in: Vector[FROM]): Array[_] =
    in.view
      .map(in => extractor(in).map(x => x: java.lang.Integer).orNull)
      .toArray
}

final case class PGBoolean[FROM](fieldName: String, extractor: FROM => Boolean)
    extends PGField[FROM, Boolean] {
  override def generateArray(in: Vector[FROM]): Array[_] = in.view.map(extractor).toArray
}

final case class PGBooleanOptional[FROM](fieldName: String, extractor: FROM => Option[Boolean])
    extends PGField[FROM, Option[Boolean]] {
  override def generateArray(in: Vector[FROM]): Array[_] =
    in.view
      .map(in => extractor(in).map(x => x: java.lang.Boolean).orNull)
      .toArray
}
