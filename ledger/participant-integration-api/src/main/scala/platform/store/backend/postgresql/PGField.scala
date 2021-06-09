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
private[postgresql] sealed abstract class PGField[FROM, TO, CONVERTED](implicit
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

private[postgresql] sealed abstract class TrivialPGField[FROM, TO](implicit classTag: ClassTag[TO])
    extends PGField[FROM, TO, TO] {
  override def convert: TO => TO = identity
}

private[postgresql] sealed trait TrivialOptionalPGField[FROM, TO >: Null <: AnyRef]
    extends PGField[FROM, Option[TO], TO] {
  override def convert: Option[TO] => TO = _.orNull
}

private[postgresql] sealed trait PGTimestampBase[FROM, TO] extends PGField[FROM, TO, String] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"$inputFieldName::timestamp"

  private val PGTimestampFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")

  protected def convertBase: Instant => String =
    _.atZone(ZoneOffset.UTC).toLocalDateTime
      .format(PGTimestampFormat)
}

private[postgresql] final case class PGTimestamp[FROM](extract: FROM => Instant)
    extends PGTimestampBase[FROM, Instant] {
  override def convert: Instant => String = convertBase
}

private[postgresql] final case class PGTimestampOptional[FROM](extract: FROM => Option[Instant])
    extends PGTimestampBase[FROM, Option[Instant]] {
  override def convert: Option[Instant] => String = _.map(convertBase).orNull
}

private[postgresql] final case class PGString[FROM](extract: FROM => String)
    extends TrivialPGField[FROM, String]

private[postgresql] final case class PGStringOptional[FROM](extract: FROM => Option[String])
    extends TrivialOptionalPGField[FROM, String]

private[postgresql] sealed trait PGStringArrayBase[FROM, TO] extends PGField[FROM, TO, String] {
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

private[postgresql] final case class PGStringArray[FROM](extract: FROM => Iterable[String])
    extends PGStringArrayBase[FROM, Iterable[String]] {
  override def convert: Iterable[String] => String = convertBase
}

private[postgresql] final case class PGStringArrayOptional[FROM](
    extract: FROM => Option[Iterable[String]]
) extends PGStringArrayBase[FROM, Option[Iterable[String]]] {
  override def convert: Option[Iterable[String]] => String = _.map(convertBase).orNull
}

private[postgresql] final case class PGBytea[FROM](extract: FROM => Array[Byte])
    extends TrivialPGField[FROM, Array[Byte]]

private[postgresql] final case class PGByteaOptional[FROM](extract: FROM => Option[Array[Byte]])
    extends TrivialOptionalPGField[FROM, Array[Byte]]

private[postgresql] final case class PGIntOptional[FROM](extract: FROM => Option[Int])
    extends PGField[FROM, Option[Int], java.lang.Integer] {
  override def convert: Option[Int] => Integer = _.map(Int.box).orNull
}

private[postgresql] final case class PGBigint[FROM](extract: FROM => Long)
    extends TrivialPGField[FROM, Long]

private[postgresql] final case class PGSmallintOptional[FROM](extract: FROM => Option[Int])
    extends PGField[FROM, Option[Int], java.lang.Integer] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"$inputFieldName::smallint"

  override def convert: Option[Int] => Integer = _.map(Int.box).orNull
}

private[postgresql] final case class PGBoolean[FROM](extract: FROM => Boolean)
    extends TrivialPGField[FROM, Boolean]

private[postgresql] final case class PGBooleanOptional[FROM](extract: FROM => Option[Boolean])
    extends PGField[FROM, Option[Boolean], java.lang.Boolean] {
  override def convert: Option[Boolean] => lang.Boolean = _.map(Boolean.box).orNull
}
