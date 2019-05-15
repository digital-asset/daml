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

object MatchingStringModule extends (String => StringModule) {

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

sealed abstract class ConcatenableMatchingStringModule extends StringModule {

  final def concat(s: T, ss: T*): T = {
    val b = newBuilder
    b += s
    ss.foreach(b += _)
    b.result()
  }

  def newBuilder: mutable.Builder[T, T]
}

object ConcatenableMatchingStringModule extends ((Char => Boolean) => StringModule) {

  def apply(pred: Char => Boolean): ConcatenableMatchingStringModule =
    new ConcatenableMatchingStringModule {
      type T = String

      def fromString(s: String): Either[String, T] =
        if (s.isEmpty)
          Left(s"""empty string""")
        else
          s.find(s => !pred(s))
            .fold[Either[String, T]](Right(s))(c =>
              Left(s"""non expected character 0x${c.toInt.toHexString} in "$s""""))

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
