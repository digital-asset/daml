// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.lang
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.reflect.ClassTag

sealed abstract class PGField[FROM, TO, CONVERTED](implicit classTag: ClassTag[CONVERTED]) {
  def extract: FROM => TO
  def convert: TO => CONVERTED
  def selectFieldExpression(inputFieldName: String): String = inputFieldName

  final def toArray(input: Vector[FROM]): Array[CONVERTED] =
    input.view
      .map {
        case notNull if notNull != null => convert(extract(notNull))
      }
      .toArray(classTag)
}

sealed abstract class TrivialPGField[FROM, TO](implicit classTag: ClassTag[TO])
    extends PGField[FROM, TO, TO] {
  override def convert: TO => TO = identity
}

final case class PGTimestamp[FROM](extract: FROM => Instant)
    extends PGField[FROM, Instant, String] {

  override def selectFieldExpression(inputFieldName: String): String =
    s"$inputFieldName::timestamp"

  override def convert: Instant => String =
    _.atZone(ZoneOffset.UTC).toLocalDateTime
      .format(PGTimestamp.PGTimestampFormat)
}

object PGTimestamp {
  private val PGTimestampFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
}

final case class PGString[FROM](extract: FROM => String) extends TrivialPGField[FROM, String]

final case class PGStringArray[FROM](extract: FROM => Iterable[String])
    extends PGField[FROM, Iterable[String], String] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"string_to_array($inputFieldName, '|')"

  override def convert: Iterable[String] => String =
    _.mkString("|") // FIXME safeguard/escape pipe chars in from
}

final case class PGBytea[FROM](extract: FROM => Array[Byte])
    extends TrivialPGField[FROM, Array[Byte]]

final case class PGIntOptional[FROM](extract: FROM => Option[Int])
    extends PGField[FROM, Option[Int], java.lang.Integer] {
  override def convert: Option[Int] => Integer = _.map(x => x: java.lang.Integer).orNull
}

final case class PGBigint[FROM](extract: FROM => Long) extends TrivialPGField[FROM, Long]

final case class PGSmallintOptional[FROM](extract: FROM => Option[Int])
    extends PGField[FROM, Option[Int], java.lang.Integer] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"$inputFieldName::smallint"

  override def convert: Option[Int] => Integer = _.map(x => x: java.lang.Integer).orNull
}

final case class PGBoolean[FROM](extract: FROM => Boolean) extends TrivialPGField[FROM, Boolean]

final case class PGBooleanOptional[FROM](extract: FROM => Option[Boolean])
    extends PGField[FROM, Option[Boolean], java.lang.Boolean] {
  override def convert: Option[Boolean] => lang.Boolean = _.map(x => x: java.lang.Boolean).orNull
}
