// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.daml.lf.data

import scalaz.Equal

import scala.collection.mutable

sealed abstract class StringModule {

  type T <: String

  def fromString(s: String): Either[String, T]

  @throws[IllegalArgumentException]
  final def assertFromString(s: String): T =
    assert(fromString(s))

  def equalInstance: Equal[T]

  // We provide the following array factory instead of a ClassTag
  // because the latter lets people easily reinterpret any string as a T.
  // See
  //  * https://github.com/digital-asset/daml/pull/983#discussion_r282513324
  //  * https://github.com/scala/bug/issues/9565
  val Array: ArrayFactory[T]

  def toStringMap[V](map: Map[T, V]): Map[String, V]
}

object MatchingStringModule {

  def apply(string_regex: String): StringModule = new StringModule {
    type T = String

    private val regex = string_regex.r
    private val pattern = regex.pattern

    def fromString(s: String): Either[String, T] =
      Either.cond(pattern.matcher(s).matches(), s, s"""string "$s" does not match regex "$regex"""")

    def equalInstance: Equal[T] = scalaz.std.string.stringInstance

    val Array: ArrayFactory[T] = new ArrayFactory[T]

    def toStringMap[V](map: Map[T, V]): Map[String, V] = map
  }

}

/** ConcatenableMatchingString are non empty US-ASCII strings built with letters, digits,
  * and some other (parameterizable) extra characters.
  * We use them to represent identifiers. In this way, we avoid
  * empty identifiers, escaping problems, and other similar pitfalls.
  */
sealed abstract class ConcatenableMatchingStringModule extends StringModule {

  def fromLong(i: Long): T

  final def fromInt(i: Int): T = fromLong(i.toLong)

  final def concat(s: T, ss: T*): T = {
    val b = newBuilder
    b += s
    ss.foreach(b += _)
    b.result()
  }

  def newBuilder: mutable.Builder[T, T]

}

object ConcatenableMatchingStringModule {

  def apply(
      extraChars: Char => Boolean,
      maxLength: Int = Int.MaxValue
  ): ConcatenableMatchingStringModule =
    new ConcatenableMatchingStringModule {
      type T = String

      def fromString(s: String): Either[String, T] =
        if (s.isEmpty)
          Left(s"""empty string""")
        else if (s.length > maxLength)
          Left(s"""string too long""")
        else
          s.find(c => c > '\u007f' || !(c.isLetterOrDigit || extraChars(c)))
            .fold[Either[String, T]](Right(s))(c =>
              Left(s"""non expected character 0x${c.toInt.toHexString} in "$s""""))

      def fromLong(i: Long): T = i.toString

      def equalInstance: Equal[T] = scalaz.std.string.stringInstance

      val Array: ArrayFactory[T] = new ArrayFactory[T]

      def toStringMap[V](map: Map[T, V]): Map[String, V] = map

      def newBuilder: mutable.ReusableBuilder[T, T] = new mutable.ReusableBuilder[T, T] {
        private val stringBuilder = StringBuilder.newBuilder
        def +=(elem: T): this.type = {
          stringBuilder.appendAll(elem)
          this
        }

        def clear(): Unit = stringBuilder.clear()

        def result(): T = stringBuilder.result()
      }
    }

}
