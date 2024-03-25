// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package magnolify.scalacheck.shrink

import org.scalacheck.Shrink
import org.scalacheck.util.Buildable

import scala.annotation.nowarn
import scala.concurrent.duration.{Duration, FiniteDuration}

/** A copy of [[org.scalacheck.Shrink]] so that we can get rid of the [[org.scalacheck.Shrink.shrinkAny]]
  * implicit that would be picked up by the derivation macro. Unfortunately, there does not seem
  * to be any way to prevent the compiler from picking up this implicit from the companion object.
  * Even introducing a another copy that causes an ambiguity does not seem to work.
  */
trait DerivedShrink[A] {
  def shrink: Shrink[A]
}

@nowarn("cat=deprecation")
object DerivedShrink {
  def apply[A](implicit ev: DerivedShrink[A]): DerivedShrink[A] = ev

  def of[A](s: Shrink[A]): DerivedShrink[A] = new DerivedShrink[A] {
    override def shrink: Shrink[A] = s
  }

  def from[A](f: A => Stream[A]): DerivedShrink[A] = of(Shrink(f))

  // Copies of the pre-defined shrink instances
  // No need to copy over the implicits for Tuples and Either as they can be auto-derived.

  implicit def shrinkContainer[C[_], T](implicit
      v: C[T] => Traversable[T],
      s: DerivedShrink[T],
      b: Buildable[T, C[T]],
  ): DerivedShrink[C[T]] = of(Shrink.shrinkContainer[C, T](v, s.shrink, b))

  implicit def shrinkContainer2[C[_, _], T, U](implicit
      v: C[T, U] => Traversable[(T, U)],
      s: DerivedShrink[(T, U)],
      b: Buildable[(T, U), C[T, U]],
  ): DerivedShrink[C[T, U]] = of(Shrink.shrinkContainer2[C, T, U](v, s.shrink, b))

  implicit def shrinkFractional[T: Fractional]: DerivedShrink[T] =
    of(Shrink.shrinkFractional[T])

  implicit def shrinkIntegral[T: Integral]: DerivedShrink[T] =
    of(Shrink.shrinkIntegral[T])

  implicit lazy val shrinkString: DerivedShrink[String] = of(Shrink.shrinkString)

  // Not equivalent to the auto-generated one because Some can be shrunk to None
  implicit def shrinkOption[T: DerivedShrink]: DerivedShrink[Option[T]] =
    of(Shrink.shrinkOption[T](DerivedShrink[T].shrink))

  implicit val shrinkFiniteDuration: DerivedShrink[FiniteDuration] =
    of(Shrink.shrinkFiniteDuration)

  implicit val shrinkDuration: DerivedShrink[Duration] = of(Shrink.shrinkDuration)
}
