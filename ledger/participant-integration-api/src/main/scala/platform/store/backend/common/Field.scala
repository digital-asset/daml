// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.lang
import java.time.Instant

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

case class StringField[FROM](extract: FROM => String) extends TrivialField[FROM, String]

case class StringOptional[FROM](extract: FROM => Option[String])
    extends TrivialOptionalField[FROM, String]

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

case class Timestamp[FROM](extract: FROM => Instant) extends TrivialField[FROM, Instant]

case class TimestampOptional[FROM](extract: FROM => Option[Instant])
    extends TrivialOptionalField[FROM, Instant]

case class StringArray[FROM](extract: FROM => Iterable[String])
    extends Field[FROM, Iterable[String], Array[String]] {
  override def convert: Iterable[String] => Array[String] = _.toArray
}

case class StringArrayOptional[FROM](extract: FROM => Option[Iterable[String]])
    extends Field[FROM, Option[Iterable[String]], Array[String]] {
  override def convert: Option[Iterable[String]] => Array[String] = _.map(_.toArray).orNull
}
