// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  /** The minimum version of this range.
    * @throws NoSuchElementException if the range is empty or unbounded below.
    */
  def min: V = minOption.getOrElse(
    throw new NoSuchElementException(s"Cannot get .min from $pretty (empty or unbounded below)")
  )

  /** The maximum version of this range.
    * @throws NoSuchElementException if the range is empty or unbounded above.
    */
  def max: V = maxOption.getOrElse(
    throw new NoSuchElementException(s"Cannot get .max from $pretty (empty or unbounded above)")
  )

  def contains(v: V): Boolean
  def map[W](f: V => W)(implicit ordering: Ordering[W]): VersionRange[W]
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

    override def contains(v: V): Boolean = false
    override def map[W](f: V => W)(implicit ordering: Ordering[W]): VersionRange.Empty[W] =
      VersionRange.Empty[W]()
  }

  /** Represents a one-sided range `[min..]`. */
  final case class From[V: Ordering](lowerBound: V) extends VersionRange[V] {
    def pretty: String = s"[${lowerBound}..] (all from ${lowerBound} upwards)"
    override def minOption: Option[V] = Some(lowerBound)
    override def maxOption: Option[V] = None

    override def min: V = lowerBound

    override def contains(v: V): Boolean = ordering.gteq(v, lowerBound)
    override def map[W](f: V => W)(implicit ordering: Ordering[W]): VersionRange.From[W] =
      VersionRange.From(f(lowerBound))
  }

  /** Represents a one-sided range `[..max]`. */
  final case class Until[V: Ordering](upperBound: V) extends VersionRange[V] {
    def pretty: String = s"[..${upperBound}] (all up to and including ${upperBound})"
    override def minOption: Option[V] = None
    override def maxOption: Option[V] = Some(upperBound)

    override def max: V = upperBound

    override def contains(v: V): Boolean = ordering.lteq(v, upperBound)
    override def map[W](f: V => W)(implicit ordering: Ordering[W]): VersionRange.Until[W] =
      VersionRange.Until(f(upperBound))
  }

  /** Represents an inclusive range `[min, max]`. This is the default */
  final case class Inclusive[V: Ordering](lowerBound: V, upperBound: V) extends VersionRange[V] {
    require(
      ordering.lteq(lowerBound, upperBound),
      s"Invalid ordering, min (${lowerBound}) was not <= max (${upperBound})",
    )

    def pretty: String = s"[${lowerBound}..${upperBound}]"

    override def minOption: Option[V] = Some(lowerBound)
    override def maxOption: Option[V] = Some(upperBound)

    override def min: V = lowerBound
    override def max: V = upperBound

    override def contains(v: V): Boolean =
      ordering.gteq(v, lowerBound) && ordering.lteq(v, upperBound)
    override def map[W](f: V => W)(implicit ordering: Ordering[W]): VersionRange.Inclusive[W] =
      VersionRange.Inclusive(f(lowerBound), f(upperBound))
  }
}
