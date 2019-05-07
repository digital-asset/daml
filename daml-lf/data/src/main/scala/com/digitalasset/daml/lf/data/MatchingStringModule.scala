// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scalaz.Equal

import scala.reflect.ClassTag
import scala.util.matching.Regex

sealed abstract class MatchingStringModule {
  type T <: String

  def fromString(s: String): Either[String, T]

  @throws[IllegalArgumentException]
  def assertFromString(s: String): T =
    fromString(s).fold(e => throw new IllegalArgumentException(e), identity)

  def unapply(x: T): Some[String] = Some(x)

  implicit def equalInstance: Equal[T]

  implicit def classTag: ClassTag[T]
}

object MatchingStringModule extends (Regex => MatchingStringModule) {

  private val classTagString = scala.reflect.classTag[String]

  override def apply(regex: Regex): MatchingStringModule = new MatchingStringModule {
    type T = String

    private val pattern = regex.pattern

    def fromString(s: String): Either[String, T] =
      Either.cond(pattern.matcher(s).matches(), s, s"""string "$s" does not match regex "$regex"""")

    implicit def equalInstance: Equal[T] = scalaz.std.string.stringInstance

    implicit def classTag: ClassTag[T] = classTagString
  }

}
