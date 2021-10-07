// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.lang
import java.sql.PreparedStatement
import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.reflect.ClassTag

/** @tparam FROM is an arbitrary type from which we can extract the data of interest for the particular column
  * @tparam TO is the intermediary type of the result of the extraction.
  *            FROM => TO functionality is intended to be injected at Schema definition time.
  *            TO is not nullable, should express a clean Scala type
  * @tparam CONVERTED is the (possibly primitive) type needed by the JDBC API
  *                   TO => CONVERTED is intended to be injected at PGField definition time.
  *                   CONVERTED might be nullable, primitive, boxed-type, whatever the JDBC API requires
  */
private[backend] abstract class Field[FROM, TO, CONVERTED](implicit
    classTag: ClassTag[CONVERTED]
) {
  def extract: (String => Int) => FROM => TO
  def convert: TO => CONVERTED
  def selectFieldExpression(inputFieldName: String): String = inputFieldName

  final def toArray(
      input: Vector[FROM],
      resolveStringInterningId: String => Int,
  ): Array[CONVERTED] =
    input.view
      .map(extract(resolveStringInterningId) andThen convert)
      .toArray(classTag)

  final def prepareData(preparedStatement: PreparedStatement, index: Int, value: Any): Unit =
    prepareDataTemplate(
      preparedStatement,
      index,
      value.asInstanceOf[CONVERTED],
    ) // this cast is safe by design

  def prepareDataTemplate(
      preparedStatement: PreparedStatement,
      index: Int,
      value: CONVERTED,
  ): Unit =
    preparedStatement.setObject(index, value)
}

private[backend] abstract class TrivialField[FROM, TO](implicit classTag: ClassTag[TO])
    extends Field[FROM, TO, TO] {
  override def convert: TO => TO = identity
}

private[backend] trait TrivialOptionalField[FROM, TO >: Null <: AnyRef]
    extends Field[FROM, Option[TO], TO] {
  override def convert: Option[TO] => TO = _.orNull
}

private[backend] case class StringField[FROM](extract: (String => Int) => FROM => String)
    extends TrivialField[FROM, String]

private[backend] case class StringOptional[FROM](extract: (String => Int) => FROM => Option[String])
    extends TrivialOptionalField[FROM, String]

private[backend] case class Bytea[FROM](extract: (String => Int) => FROM => Array[Byte])
    extends TrivialField[FROM, Array[Byte]]

private[backend] case class ByteaOptional[FROM](
    extract: (String => Int) => FROM => Option[Array[Byte]]
) extends TrivialOptionalField[FROM, Array[Byte]]

private[backend] case class Integer[FROM](extract: (String => Int) => FROM => Int)
    extends TrivialField[FROM, Int]

private[backend] case class IntOptional[FROM](extract: (String => Int) => FROM => Option[Int])
    extends Field[FROM, Option[Int], java.lang.Integer] {
  override def convert: Option[Int] => java.lang.Integer = _.map(Int.box).orNull
}

private[backend] case class Bigint[FROM](extract: (String => Int) => FROM => Long)
    extends TrivialField[FROM, Long]

private[backend] case class BigintOptional[FROM](extract: (String => Int) => FROM => Option[Long])
    extends Field[FROM, Option[Long], java.lang.Long] {
  override def convert: Option[Long] => java.lang.Long = _.map(Long.box).orNull
}

private[backend] case class SmallintOptional[FROM](extract: (String => Int) => FROM => Option[Int])
    extends Field[FROM, Option[Int], java.lang.Integer] {
  override def convert: Option[Int] => java.lang.Integer = _.map(Int.box).orNull
}

private[backend] case class BooleanField[FROM](extract: (String => Int) => FROM => Boolean)
    extends TrivialField[FROM, Boolean]

private[backend] case class BooleanOptional[FROM](
    extract: (String => Int) => FROM => Option[Boolean]
) extends Field[FROM, Option[Boolean], java.lang.Boolean] {
  override def convert: Option[Boolean] => lang.Boolean = _.map(Boolean.box).orNull
}

private[backend] object Timestamp {
  def instantToMicros(i: Instant): Long =
    TimeUnit.SECONDS.toMicros(i.getEpochSecond) + TimeUnit.NANOSECONDS.toMicros(i.getNano.toLong)
}

private[backend] case class StringArray[FROM](extract: (String => Int) => FROM => Iterable[String])
    extends Field[FROM, Iterable[String], Array[String]] {
  override def convert: Iterable[String] => Array[String] = _.toArray
}

private[backend] case class StringArrayOptional[FROM](
    extract: (String => Int) => FROM => Option[Iterable[String]]
) extends Field[FROM, Option[Iterable[String]], Array[String]] {
  override def convert: Option[Iterable[String]] => Array[String] = _.map(_.toArray).orNull
}

private[backend] case class IntArray[FROM](extract: (String => Int) => FROM => Iterable[Int])
    extends Field[FROM, Iterable[Int], Array[Int]] {
  override def convert: Iterable[Int] => Array[Int] = _.toArray
}

private[backend] case class IntArrayOptional[FROM](
    extract: (String => Int) => FROM => Option[Iterable[Int]]
) extends Field[FROM, Option[Iterable[Int]], Array[Int]] {
  override def convert: Option[Iterable[Int]] => Array[Int] = _.map(_.toArray).orNull
}
