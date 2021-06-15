// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.lang
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.reflect.ClassTag

/** @tparam FROM is an arbitrary type from which we can extract the data of interest for the particular column
  * @tparam TO is the intermediary type of the result of the extraction.
  *            FROM => TO functionality is intended to be injected at Schema definition time.
  *            TO is not nullable, should express a clean Scala type
  * @tparam CONVERTED is the (possibly primitive) type needed by the JDBC API
  *                   TO => CONVERTED is intended to be injected at PGField definition time.
  *                   CONVERTED might be nullable, primitive, boxed-type, whatever the JDBC API requires
  */
abstract class Field[FROM, TO, CONVERTED](implicit
    classTag: ClassTag[CONVERTED]
) {
  def extract: FROM => TO
  def convert: TO => CONVERTED
  def selectFieldExpression(inputFieldName: String): String = inputFieldName

  final def toArray(input: Vector[FROM]): Array[CONVERTED] =
    input.view
      .map(extract andThen convert)
      .toArray(classTag)
}

abstract class TrivialField[FROM, TO](implicit classTag: ClassTag[TO]) extends Field[FROM, TO, TO] {
  override def convert: TO => TO = identity
}

trait TrivialOptionalField[FROM, TO >: Null <: AnyRef] extends Field[FROM, Option[TO], TO] {
  override def convert: Option[TO] => TO = _.orNull
}

private[postgresql] trait PGTimestampBase[FROM, TO] extends Field[FROM, TO, String] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"$inputFieldName::timestamp"

  private val PGTimestampFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")

  protected def convertBase: Instant => String =
    _.atZone(ZoneOffset.UTC).toLocalDateTime
      .format(PGTimestampFormat)
}

private[postgresql] case class PGTimestamp[FROM](extract: FROM => Instant)
    extends PGTimestampBase[FROM, Instant] {
  override def convert: Instant => String = convertBase
}

private[postgresql] case class PGTimestampOptional[FROM](extract: FROM => Option[Instant])
    extends PGTimestampBase[FROM, Option[Instant]] {
  override def convert: Option[Instant] => String = _.map(convertBase).orNull
}

case class StringField[FROM](extract: FROM => String) extends TrivialField[FROM, String]

case class StringOptional[FROM](extract: FROM => Option[String])
    extends TrivialOptionalField[FROM, String]

private[postgresql] trait PGStringArrayBase[FROM, TO] extends Field[FROM, TO, String] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"string_to_array($inputFieldName, '|')"

  protected def convertBase: Iterable[String] => String = { in =>
    assert(
      in.forall(!_.contains("|")),
      s"The following input string(s) contain the character '|', which is not expected: ${in.filter(_.contains("|")).mkString(", ")}",
    )
    in.mkString("|")
  }
}

private[postgresql] case class PGStringArray[FROM](extract: FROM => Iterable[String])
    extends PGStringArrayBase[FROM, Iterable[String]] {
  override def convert: Iterable[String] => String = convertBase
}

private[postgresql] case class PGStringArrayOptional[FROM](
    extract: FROM => Option[Iterable[String]]
) extends PGStringArrayBase[FROM, Option[Iterable[String]]] {
  override def convert: Option[Iterable[String]] => String = _.map(convertBase).orNull
}

case class Bytea[FROM](extract: FROM => Array[Byte]) extends TrivialField[FROM, Array[Byte]]

case class ByteaOptional[FROM](extract: FROM => Option[Array[Byte]])
    extends TrivialOptionalField[FROM, Array[Byte]]

case class IntOptional[FROM](extract: FROM => Option[Int])
    extends Field[FROM, Option[Int], java.lang.Integer] {
  override def convert: Option[Int] => Integer = _.map(Int.box).orNull
}

case class Bigint[FROM](extract: FROM => Long) extends TrivialField[FROM, Long]

case class SmallintOptional[FROM](extract: FROM => Option[Int])
    extends Field[FROM, Option[Int], java.lang.Integer] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"$inputFieldName::smallint"

  override def convert: Option[Int] => Integer = _.map(Int.box).orNull
}

case class BooleanField[FROM](extract: FROM => Boolean) extends TrivialField[FROM, Boolean]

case class BooleanOptional[FROM](extract: FROM => Option[Boolean])
    extends Field[FROM, Option[Boolean], java.lang.Boolean] {
  override def convert: Option[Boolean] => lang.Boolean = _.map(Boolean.box).orNull
}
