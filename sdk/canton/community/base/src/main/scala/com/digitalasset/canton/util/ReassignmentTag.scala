// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.{Applicative, Eval, Monad, Traverse}
import com.digitalasset.canton.AllowTraverseSingleContainer
import com.digitalasset.canton.logging.pretty.Pretty
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

/** In reassignment transactions, we deal with two domains: the source domain and the target domain.
  * The `Source` and `Target` wrappers help differentiate between these two domains, allowing us to manage
  * their specific characteristics, such as protocol versions, static domain parameters, and other domain-specific details.
  */
sealed trait ReassignmentTag[+T] extends Product with Serializable {
  def unwrap: T
}

object ReassignmentTag {
  final case class Source[+T](value: T) extends ReassignmentTag[T] {
    override def unwrap: T = value
  }

  object Source {
    implicit def ordering[T: Ordering]: Ordering[Source[T]] =
      Ordering.by(_.unwrap)

    implicit def pretty[T: Pretty]: Pretty[Source[T]] = (t: Source[T]) => Pretty[T].treeOf(t.unwrap)

    implicit def setParameter[T](implicit s: SetParameter[T]): SetParameter[Source[T]] =
      (d: Source[T], pp: PositionedParameters) => pp >> d.unwrap

    implicit def getResult[T: GetResult]: GetResult[Source[T]] = GetResult[T].andThen(Source(_))

    @AllowTraverseSingleContainer
    implicit def sourceMonadInstance: Monad[Source] & Traverse[Source] = new Monad[Source]
      with Traverse[Source] {
      override def pure[A](x: A): Source[A] = Source(x)
      override def flatMap[A, B](fa: Source[A])(f: A => Source[B]): Source[B] = f(fa.value)
      override def tailRecM[A, B](a: A)(f: A => Source[Either[A, B]]): Source[B] = {
        @annotation.tailrec
        def loop(a: A): Source[B] = f(a) match {
          case Source(Left(nextA)) => loop(nextA)
          case Source(Right(b)) => Source(b)
        }
        loop(a)
      }
      override def traverse[G[_]: Applicative, A, B](fa: Source[A])(f: A => G[B]): G[Source[B]] =
        Applicative[G].map(f(fa.value))(Source(_))
      override def foldLeft[A, B](fa: Source[A], b: B)(f: (B, A) => B): B = f(b, fa.value)
      override def foldRight[A, B](fa: Source[A], lb: Eval[B])(
          f: (A, Eval[B]) => Eval[B]
      ): Eval[B] =
        f(fa.value, lb)
    }

    implicit def sourceReassignmentType: SameReassignmentType[Source] =
      new SameReassignmentType[Source] {}
  }

  final case class Target[+T](value: T) extends ReassignmentTag[T] {
    override def unwrap: T = value
  }

  object Target {
    implicit def ordering[T: Ordering]: Ordering[Target[T]] =
      Ordering.by(_.unwrap)

    implicit def pretty[T: Pretty]: Pretty[Target[T]] = (t: Target[T]) => Pretty[T].treeOf(t.unwrap)

    implicit def setParameter[T](implicit s: SetParameter[T]): SetParameter[Target[T]] =
      (d: Target[T], pp: PositionedParameters) => pp >> d.unwrap

    implicit def getResult[T: GetResult]: GetResult[Target[T]] = GetResult[T].andThen(Target(_))

    @AllowTraverseSingleContainer
    implicit def targetMonadInstance: Monad[Target] & Traverse[Target] = new Monad[Target]
      with Traverse[Target] {
      override def pure[A](x: A): Target[A] = Target(x)
      override def flatMap[A, B](fa: Target[A])(f: A => Target[B]): Target[B] = f(fa.value)
      override def tailRecM[A, B](a: A)(f: A => Target[Either[A, B]]): Target[B] = {
        @annotation.tailrec
        def loop(a: A): Target[B] = f(a) match {
          case Target(Left(nextA)) => loop(nextA)
          case Target(Right(b)) => Target(b)
        }
        loop(a)
      }
      override def traverse[G[_]: Applicative, A, B](fa: Target[A])(f: A => G[B]): G[Target[B]] =
        Applicative[G].map(f(fa.value))(Target(_))
      override def foldLeft[A, B](fa: Target[A], b: B)(f: (B, A) => B): B = f(b, fa.value)
      override def foldRight[A, B](fa: Target[A], lb: Eval[B])(
          f: (A, Eval[B]) => Eval[B]
      ): Eval[B] = f(fa.value, lb)
    }

    implicit val targetReassignmentType: SameReassignmentType[Target] =
      new SameReassignmentType[Target] {}
  }
}

/** A type class that ensures the reassignment type remains consistent across multiple parameters of a method.
  * This is useful when dealing with types that represent different reassignment contexts (e.g., `Source` and `Target`),
  * and we want to enforce that all parameters share the same reassignment context.
  *
  * Example:
  *
  * def f[F[_] <: ReassignmentTag[_]: SameReassignmentType](i: F[Int], s: F[String]) = ???
  *
  * // f(Source(1), Target("One"))  // This will not compile, as `Source` and `Target` are different reassignment types.
  * // f(Source(1), Source("One"))  // This will compile, as both parameters are of the same reassignment type `Source`.
  */
trait SameReassignmentType[T[_]] {}
