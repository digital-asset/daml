// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

/** [[VersionRange]] represents a range of versions any type but usually of
  * [[com.digitalasset.daml.lf.LanguageVersion]] or
  * [[com.digitalasset.daml.lf.SerializationVersion]].
  *
  * This can represent an empty range, a one-sided range (from/until), or
  * an inclusive two-sided range.
  *
  * @tparam V either [[com.digitalasset.daml.lf.LanguageVersion]] or
  * [[com.digitalasset.daml.lf.SerializationVersion]].
  */
sealed abstract class VersionRange[V](implicit val ordering: Ordering[V])
    extends Product
    with Serializable {

  def pretty: String

  def contains(v: V): Boolean = this match {
    case VersionRange.Empty() => false
    case VersionRange.From(min) => ordering.gteq(v, min)
    case VersionRange.Until(max) => ordering.lteq(v, max)
    case VersionRange.Inclusive(min, max) => ordering.gteq(v, min) && ordering.lteq(v, max)
  }

  def map[W](f: V => W)(implicit ordering: Ordering[W]): VersionRange[W] = this match {
    case VersionRange.Empty() => VersionRange.Empty[W]()
    case VersionRange.From(min) => VersionRange.From(f(min))
    case VersionRange.Until(max) => VersionRange.Until(f(max))
    case VersionRange.Inclusive(min, max) => VersionRange.Inclusive(f(min), f(max))
  }
}

object VersionRange {

  /** The default `apply` constructor creates an inclusive range,
    * preserving the behavior of the old case class (which only supported inclusive ranges).
    */
  def apply[V: Ordering](min: V, max: V): VersionRange.Inclusive[V] =
    Inclusive(min, max)

  final case class Empty[V: Ordering]() extends VersionRange[V] {
    def pretty: String = "[] (empty range)"
  }

  /** Represents a one-sided range `[min..]`. */
  final case class From[V: Ordering](min: V) extends VersionRange[V] {
    def pretty: String = s"[${min}..] (all from ${min} upwards)"
  }

  /** Represents a one-sided range `[..max]`. */
  final case class Until[V: Ordering](max: V) extends VersionRange[V] {
    def pretty: String = s"[..${max}] (all up to and including ${max})"
  }

  /** Represents an inclusive range `[min, max]`. This is the default */
  final case class Inclusive[V: Ordering](min: V, max: V) extends VersionRange[V] {
    require(ordering.lteq(min, max), s"Invalid ordering, min (${min}) was not <= max (${max})")

    def pretty: String = s"[${min}..${max}])"

    // override map to get a map from inclusive to inclusive
    override def map[W](f: V => W)(implicit ordering: Ordering[W]): VersionRange.Inclusive[W] =
      VersionRange.Inclusive(f(min), f(max))
  }
}
