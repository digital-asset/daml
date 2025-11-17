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

  /** The minimum version of this range, if bounded below. */
  def minOption: Option[V]

  /** The maximum version of this range, if bounded above. */
  def maxOption: Option[V]

  def contains(v: V): Boolean = this match {
    case VersionRange.Empty() => false
    case VersionRange.From(lowerBound) => ordering.gteq(v, lowerBound)
    case VersionRange.Until(upperBound) => ordering.lteq(v, upperBound)
    case VersionRange.Inclusive(lowerBound, upperBound) =>
      ordering.gteq(v, lowerBound) && ordering.lteq(v, upperBound)
  }

  def map[W](f: V => W)(implicit ordering: Ordering[W]): VersionRange[W] = this match {
    case VersionRange.Empty() => VersionRange.Empty[W]()
    case VersionRange.From(lowerBound) => VersionRange.From(f(lowerBound))
    case VersionRange.Until(upperBound) => VersionRange.Until(f(upperBound))
    case VersionRange.Inclusive(lowerBound, upperBound) =>
      VersionRange.Inclusive(f(lowerBound), f(upperBound))
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
    override def minOption: Option[V] = None
    override def maxOption: Option[V] = None
  }

  /** Represents a one-sided range `[min..]`. */
  final case class From[V: Ordering](min: V) extends VersionRange[V] {
    def pretty: String = s"[${min}..] (all from ${min} upwards)"
    override def minOption: Option[V] = Some(min)
    override def maxOption: Option[V] = None
  }

  /** Represents a one-sided range `[..max]`. */
  final case class Until[V: Ordering](max: V) extends VersionRange[V] {
    def pretty: String = s"[..${max}] (all up to and including ${max})"
    override def minOption: Option[V] = None
    override def maxOption: Option[V] = Some(max)
  }

  /** Represents an inclusive range `[min, max]`. This is the default */
  final case class Inclusive[V: Ordering](min: V, max: V) extends VersionRange[V] {
    require(
      ordering.lteq(min, max),
      s"Invalid ordering, min (${min}) was not <= max (${max})",
    )

    def pretty: String = s"[${min}..${max}]"

    override def minOption: Option[V] = Some(min)
    override def maxOption: Option[V] = Some(max)

    // override map to get a map from inclusive to inclusive
    override def map[W](f: V => W)(implicit ordering: Ordering[W]): VersionRange.Inclusive[W] =
      VersionRange.Inclusive(f(min), f(max))
  }
}
