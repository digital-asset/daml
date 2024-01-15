// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.dotnot

/** A zipper for a property-like structure, such as dot-notation
  *
  * For instance, given the property `foo.bar.baz.doh`, a cursor `PropertyCursor(List("foo"), "bar", List("baz", "doh"))`
  * is pointing at the `"bar"` part of the property.
  *
  * @param reversedBefore the part of the property prior to the cursor
  * @param current the piece of the property currently pointed by the cursor
  * @param after the part of the property after the cursor
  */
final case class PropertyCursor(
    reversedBefore: List[String],
    current: String,
    after: List[String],
) {

  /** @return whether [[current]] is the last */
  def isLast: Boolean = after.isEmpty

  /** @return a cursor pointing at the previous part of the property, if possible */
  def prev: Option[PropertyCursor] =
    reversedBefore match {
      case Nil => None
      case head :: tail => Some(PropertyCursor(tail, head, current :: after))
    }

  /** @return a cursor pointing at the next part of the property, if possible */
  def next: Option[PropertyCursor] =
    after match {
      case Nil => None
      case head :: tail => Some(PropertyCursor(current :: reversedBefore, head, tail))
    }

  /** Change the part pointed by the cursor */
  def withCurrent(current: String): PropertyCursor =
    this.copy(current = current)

  /** Change the part of the property after the cursor */
  def withAfter(newAfter: List[String]): PropertyCursor =
    this.copy(after = newAfter)

  override def toString: String =
    (reversedBefore.reverse ++ (("[" + current + "]") +: after)).mkString(".")

  def ensureLast[A](target: String)(r: => A): Either[DotNotFailure, A] =
    if (isLast) Right(r) else Left(MustBeLastPart(target, this, current))
}

object PropertyCursor {

  /** @return a [[PropertyCursor]] created by splitting the input on dot */
  def fromString(str: String): PropertyCursor = {
    val Array(current, after @ _*) = str.split("\\.", -1)
    PropertyCursor(List.empty, current, after.toList)
  }
}
