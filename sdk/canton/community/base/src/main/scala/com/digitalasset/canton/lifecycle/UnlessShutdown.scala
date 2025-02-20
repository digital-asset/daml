// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.{Applicative, Eval, Monad, Monoid, Traverse}

import scala.annotation.tailrec
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

/** The outcome of a computation ([[UnlessShutdown.Outcome]]) unless the computation has aborted due
  * to a shutdown ([[UnlessShutdown.AbortedDueToShutdown]]).
  *
  * A copy of [[scala.Option]]. We use a separate class to document the purpose.
  *
  * @tparam A
  *   The type of the outcome.
  */
sealed trait UnlessShutdown[+A] extends Product with Serializable {

  /** Applies the function to the outcome if available */
  def foreach(f: A => Unit): Unit

  /** Transforms the outcome using the given function. */
  def map[B](f: A => B): UnlessShutdown[B]

  def forall(p: A => Boolean): Boolean

  /** Monadically chain two outcome computations. Abortion due to shutdown propagates. */
  def flatMap[B](f: A => UnlessShutdown[B]): UnlessShutdown[B]

  /** Applicative traverse for outcome computations. The given function is not applied upon
    * abortion.
    */
  def traverse[F[_], B](f: A => F[B])(implicit F: Applicative[F]): F[UnlessShutdown[B]]

  /** Convert the outcome into an [[scala.Right$]] or [[scala.Left$]]`(aborted)` upon abortion. */
  def toRight[L](aborted: => L): Either[L, A]

  /** Evaluate the argument upon abortion and otherwise return the outcome
    *
    * Analogue to [[scala.Option.getOrElse]].
    */
  def onShutdown[B >: A](ifShutdown: => B): B

  /** Returns whether the outcome is an actual outcome */
  def isOutcome: Boolean
}

object UnlessShutdown {
  final case class Outcome[+A](result: A) extends UnlessShutdown[A] {
    override def foreach(f: A => Unit): Unit = f(result)
    override def map[B](f: A => B): Outcome[B] = Outcome(f(result))
    override def flatMap[B](f: A => UnlessShutdown[B]): UnlessShutdown[B] = f(result)
    override def forall(p: A => Boolean): Boolean = p(this.result)
    override def traverse[F[_], B](f: A => F[B])(implicit F: Applicative[F]): F[UnlessShutdown[B]] =
      F.map(f(result))(Outcome(_))
    override def toRight[L](aborted: => L): Either[L, A] = Right(result)
    override def onShutdown[B >: A](ifShutdown: => B): A = result
    override def isOutcome: Boolean = true

  }

  case object AbortedDueToShutdown extends UnlessShutdown[Nothing] {
    override def foreach(f: Nothing => Unit): Unit = ()
    override def map[B](f: Nothing => B): AbortedDueToShutdown = this
    override def flatMap[B](f: Nothing => UnlessShutdown[B]): AbortedDueToShutdown = this
    override def forall(p: Nothing => Boolean): Boolean = true
    override def traverse[F[_], B](f: Nothing => F[B])(implicit
        F: Applicative[F]
    ): F[UnlessShutdown[B]] = F.pure(this)
    override def toRight[L](aborted: => L): Either[L, Nothing] = Left(aborted)
    override def onShutdown[B >: Nothing](ifShutdown: => B): B = ifShutdown
    override def isOutcome: Boolean = false
  }
  type AbortedDueToShutdown = AbortedDueToShutdown.type

  val unit: UnlessShutdown[Unit] = Outcome(())

  def fromOption[A](x: Option[A]): UnlessShutdown[A] =
    x.fold[UnlessShutdown[A]](AbortedDueToShutdown)(Outcome.apply)

  /** Converts [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]]s into an
    * internal exception so that shutdowns can tunnel through APIs that expect a plain
    * [[scala.util.Try]] Must be used together with
    * [[com.digitalasset.canton.lifecycle.UnlessShutdown.recoverFromAbortException]] to turn the
    * internal exception back into
    * [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]].
    */
  def failOnShutdownToAbortException[A](x: Try[UnlessShutdown[A]], action: String): Try[A] =
    x.flatMap {
      case Outcome(result) => Success(result)
      case AbortedDueToShutdown => Failure(AbortedDueToShutdownException(action))
    }

  def recoverFromAbortException[A](x: Try[A]): Try[UnlessShutdown[A]] =
    x.transform(
      value => Success(UnlessShutdown.Outcome(value)),
      {
        case AbortedDueToShutdownException(_) =>
          Success(UnlessShutdown.AbortedDueToShutdown)
        case other => Failure(other)
      },
    )

  /** Cats traverse and monad instance for [[UnlessShutdown]].
    *
    * [[AbortedDueToShutdown]] propagates.
    */
  implicit val catsStdInstsUnlessShutdown: Traverse[UnlessShutdown] with Monad[UnlessShutdown] =
    new Traverse[UnlessShutdown] with Monad[UnlessShutdown] {
      override def flatMap[A, B](x: UnlessShutdown[A])(
          f: A => UnlessShutdown[B]
      ): UnlessShutdown[B] = x.flatMap(f)

      override def tailRecM[A, B](a: A)(f: A => UnlessShutdown[Either[A, B]]): UnlessShutdown[B] = {
        @tailrec def go(s: A): UnlessShutdown[B] = f(s) match {
          case Outcome(Left(next)) => go(next)
          case Outcome(Right(done)) => Outcome(done)
          case AbortedDueToShutdown => AbortedDueToShutdown
        }
        go(a)
      }

      override def pure[A](x: A): UnlessShutdown[A] = Outcome(x)

      override def traverse[G[_], A, B](x: UnlessShutdown[A])(f: A => G[B])(implicit
          G: Applicative[G]
      ): G[UnlessShutdown[B]] =
        x.traverse(f)

      override def foldLeft[A, B](x: UnlessShutdown[A], b: B)(f: (B, A) => B): B = x match {
        case Outcome(result) => f(b, result)
        case AbortedDueToShutdown => b
      }

      override def foldRight[A, B](x: UnlessShutdown[A], lb: Eval[B])(
          f: (A, Eval[B]) => Eval[B]
      ): Eval[B] = x match {
        case Outcome(result) => f(result, lb)
        case AbortedDueToShutdown => lb
      }
    }

  /** Lift a [[cats.Monoid]] on outcomes to [[UnlessShutdown]]. [[AbortedDueToShutdown]] cancels.
    */
  implicit def monoidUnlessShutdown[A](implicit monoid: Monoid[A]): Monoid[UnlessShutdown[A]] =
    new Monoid[UnlessShutdown[A]] {
      override def empty: UnlessShutdown[A] = Outcome(monoid.empty)

      override def combine(x1: UnlessShutdown[A], x2: UnlessShutdown[A]): UnlessShutdown[A] =
        x1 match {
          case Outcome(y1) => x2.map(monoid.combine(y1, _))
          case AbortedDueToShutdown => AbortedDueToShutdown
        }
    }

  /** Internal exception to channel an
    * [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] through an API that
    * expects plain [[scala.concurrent.Future]] or [[scala.util.Try]]. Do not use this exception for
    * reporting a shutdown to the caller of an API.
    */
  private final case class AbortedDueToShutdownException(action: String)
      extends RuntimeException(s"'$action' was aborted due to shutdown.")
      with NoStackTrace
}
