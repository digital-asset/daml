// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.daml.lf.data

import scalaz.Equal

sealed abstract class MatchingStringModule {

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
}

object MatchingStringModule extends (String => MatchingStringModule) {

  def apply(string_regex: String): MatchingStringModule = new MatchingStringModule {
    type T = String

    private val regex = string_regex.r
    private val pattern = regex.pattern

    def fromString(s: String): Either[String, T] =
      Either.cond(pattern.matcher(s).matches(), s, s"""string "$s" does not match regex "$regex"""")

    def equalInstance: Equal[T] = scalaz.std.string.stringInstance

    val Array: ArrayFactory[T] = new ArrayFactory[T]
  }

}

sealed abstract class ConcatenableMatchingStringModule extends MatchingStringModule {

  def concat(s: T, t: T): T

}

object ConcatenableMatchingStringModule extends (String => MatchingStringModule) {

  def apply(string_regex: String): ConcatenableMatchingStringModule =
    new ConcatenableMatchingStringModule {
      type T = String

      private val regex = s"""($string_regex)+""".r
      private val pattern = regex.pattern

      def fromString(s: String): Either[String, T] =
        Either.cond(
          pattern.matcher(s).matches(),
          s,
          s"""string "$s" does not match regex "$regex"""")

      def equalInstance: Equal[T] = scalaz.std.string.stringInstance

      val Array: ArrayFactory[T] = new ArrayFactory[T]

      def concat(s: T, t: T): T = s + t
    }

}
