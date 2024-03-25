// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.{Chain, EitherT, NonEmptyChain}
import cats.{Applicative, Eval, Functor, MonadError, Now}

import scala.annotation.tailrec

/** A monad for aborting and non-aborting errors. Non-aborting errors are accumulated in a [[cats.data.Chain]]
  * until the first aborting error is hit. You can think of [[com.digitalasset.canton.util.Checked]] as an extension of
  * `Either` to also support errors that should not cause the computation to abort.
  *
  * @tparam A Type of aborting errors
  * @tparam N Type of non-aborting errors
  * @tparam R Result type of the monad
  */
sealed abstract class Checked[+A, +N, +R] extends Product with Serializable {
  import Checked.*

  def map[RR](f: R => RR): Checked[A, N, RR] = this match {
    case abort @ Abort(_, _) => abort
    case Result(nonaborts, result) => Result(nonaborts, f(result))
  }

  def mapAbort[AA](f: A => AA): Checked[AA, N, R] = this match {
    case Abort(abort, nonaborts) => Abort(f(abort), nonaborts)
    case r @ Result(_, _) => r
  }

  def mapNonaborts[NN](f: Chain[N] => Chain[NN]): Checked[A, NN, R] = this match {
    case Abort(abort, nonaborts) => Abort(abort, f(nonaborts))
    case Result(nonaborts, result) => Result(f(nonaborts), result)
  }
  def mapNonabort[NN](f: N => NN): Checked[A, NN, R] = mapNonaborts(_.map(f))

  def trimap[AA, NN, RR](
      abortMap: A => AA,
      nonabortMap: N => NN,
      resultMap: R => RR,
  ): Checked[AA, NN, RR] =
    this match {
      case Abort(abort, nonaborts) => Abort(abortMap(abort), nonaborts.map(nonabortMap))
      case Result(nonaborts, result) => Result(nonaborts.map(nonabortMap), resultMap(result))
    }

  def fold[B](f: (A, Chain[N]) => B, g: (Chain[N], R) => B): B = this match {
    case Abort(abort, nonaborts) => f(abort, nonaborts)
    case Result(nonaborts, result) => g(nonaborts, result)
  }

  def prependNonaborts[NN >: N](nonaborts: Chain[NN]): Checked[A, NN, R] = mapNonaborts(
    Chain.concat(nonaborts, _)
  )
  def prependNonabort[NN >: N](nonabort: NN): Checked[A, NN, R] = prependNonaborts(
    Chain.one(nonabort)
  )

  def appendNonaborts[NN >: N](nonaborts: Chain[NN]): Checked[A, NN, R] = mapNonaborts(
    Chain.concat(_, nonaborts)
  )
  def appendNonabort[NN >: N](nonabort: NN): Checked[A, NN, R] = appendNonaborts(
    Chain.one(nonabort)
  )

  /** Applicative product operation. Errors from `this` take precedence over `other` */
  def product[AA >: A, NN >: N, RR](other: Checked[AA, NN, RR]): Checked[AA, NN, (R, RR)] =
    this match {
      case abort @ Abort(_, _) => abort
      case Result(nonaborts1, result1) =>
        other match {
          case abort @ Abort(_, _) => abort.prependNonaborts(nonaborts1)
          case Result(nonaborts2, result2) =>
            Result(Chain.concat(nonaborts1, nonaborts2), (result1, result2))
        }
    }

  /** Applicative operation. Consistent with the monadic [[flatMap]] according to Cats' laws, i.e.,
    * {{{
    * x.ap(f) = for { g <- f; y <- x } yield g(x)
    * }}}
    */
  def ap[AA >: A, NN >: N, RR](f: Checked[AA, NN, R => RR]): Checked[AA, NN, RR] =
    f.product(this).map { case (g, x) => g(x) }

  /** Reverse applicative operation. Errors from the argument (= `this`) take precedence over those from the function. */
  def reverseAp[AA >: A, NN >: N, RR](f: Checked[AA, NN, R => RR]): Checked[AA, NN, RR] =
    this.product(f).map { case (x, g) => g(x) }

  def flatMap[AA >: A, NN >: N, RR](f: R => Checked[AA, NN, RR]): Checked[AA, NN, RR] = this match {
    case abort @ Abort(_, _) => abort
    case Result(nonaborts, result) => f(result).prependNonaborts(nonaborts)
  }

  def biflatMap[AA, NN >: N, RR](
      f: A => Checked[AA, NN, RR],
      g: R => Checked[AA, NN, RR],
  ): Checked[AA, NN, RR] =
    this match {
      case Abort(abort, nonaborts) => f(abort).prependNonaborts(nonaborts)
      case Result(nonaborts, result) => g(result).prependNonaborts(nonaborts)
    }

  def abortFlatMap[AA, NN >: N, RR >: R](f: A => Checked[AA, NN, RR]): Checked[AA, NN, RR] =
    biflatMap(f, Checked.result)

  /** Merges aborts with nonaborts, using the given `default` result if no result is contained. */
  def toResult[NN, RR >: R, A1 >: A <: NN, N1 >: N <: NN](
      default: => RR
  ): Checked[Nothing, NN, RR] = this match {
    case Abort(abort, nonaborts) => Result((nonaborts: Chain[N1]).prepend[NN](abort: A1), default)
    case result @ Result(_, _) => result: Checked[Nothing, N1, RR]
  }

  def foreach(f: R => Unit): Unit = this match {
    case Result(_, result) => f(result)
    case _ => ()
  }

  def exists(pred: R => Boolean): Boolean = this match {
    case Result(_, result) => pred(result)
    case _ => false
  }

  def forall(pred: R => Boolean): Boolean = this match {
    case Result(_, result) => pred(result)
    case _ => true
  }

  /** When [[Checked.Result]], apply the function, marking the result as [[Checked.Result]]
    * inside the Applicative's context, keeping the warnings.
    * when [[Checked.Abort]], lift the [[Checked.Abort]] into the Applicative's context
    */
  def traverse[F[_], AA >: A, NN >: N, RR](
      f: R => F[RR]
  )(implicit F: Applicative[F]): F[Checked[AA, NN, RR]] =
    this match {
      case Result(nonaborts, result) => F.map(f(result))(Result(nonaborts, _))
      case e @ Abort(_, _) => F.pure(e)
    }

  /** Discards nonaborts. */
  def toEither: Either[A, R] = this match {
    case Abort(abort, _) => Left(abort)
    case Result(_, result) => Right(result)
  }

  /** Discards the result if there are nonaborts. */
  // Specifies two lower bounds for L as described in https://stackoverflow.com/a/6124549
  def toEitherWithNonaborts[L, A1 >: A <: L, N1 >: N <: L]: Either[NonEmptyChain[L], R] =
    this match {
      case Abort(abort, nonaborts) =>
        Left(NonEmptyChain.fromChainPrepend[L](abort: A1, nonaborts: Chain[N1]))
      case Result(nonaborts, result) =>
        NonEmptyChain.fromChain[L](nonaborts: Chain[N1]).toLeft(result)
    }

  /** Discards the result if there are nonaborts. */
  def toEitherMergeNonaborts[L >: N](implicit
      ev: A <:< NonEmptyChain[L]
  ): Either[NonEmptyChain[L], R] = this match {
    case Abort(abort, nonaborts) => Left(ev(abort).appendChain[L](nonaborts: Chain[N]))
    case Result(nonaborts, result) => NonEmptyChain.fromChain[L](nonaborts: Chain[N]).toLeft(result)
  }

  def toOption: Option[R] = this match {
    case Abort(_, _) => None
    case Result(_, result) => Some(result)
  }

  def isAbort: Boolean = this match {
    case Abort(_, _) => true
    case Result(_, _) => false
  }

  def isResult: Boolean = this match {
    case Abort(_, _) => false
    case Result(_, _) => true
  }

  /** Is a [[Checked.Result]] with no errors */
  def successful: Boolean = this match {
    case Abort(_, _) => false
    case Result(nonaborts, _) => nonaborts.isEmpty
  }
  def nonaborts: Chain[N]
  def getResult: Option[R] = this match {
    case Abort(_, _) => None
    case Result(_, result) => Some(result)
  }
  def getAbort: Option[A] = this match {
    case Abort(abort, _) => Some(abort)
    case Result(_, _) => None
  }
}

object Checked {
  final case class Abort[+A, +N](abort: A, override val nonaborts: Chain[N])
      extends Checked[A, N, Nothing]
  final case class Result[+N, +R](override val nonaborts: Chain[N], result: R)
      extends Checked[Nothing, N, R]

  def abort[A, N, R](abort: A): Checked[A, N, R] = Abort(abort, Chain.empty)
  def result[A, N, R](result: R): Checked[A, N, R] = Result(Chain.empty, result)
  def continueWithResult[A, N, R](nonabort: N, result: R): Checked[A, N, R] =
    Result(Chain.one(nonabort), result)
  def continue[A, N](nonabort: N): Checked[A, N, Unit] = continueWithResult(nonabort, ())
  def continuesWithResult[A, N, R](nonaborts: NonEmptyChain[N], result: R): Checked[A, N, R] =
    Result(nonaborts.toChain, result)
  def continues[A, N](nonaborts: NonEmptyChain[N]): Checked[A, N, Unit] =
    continuesWithResult(nonaborts, ())
  def unit[A, N]: Checked[A, N, Unit] = result(())

  /** Treat [[scala.Left$]] as abort */
  def fromEither[A, R](either: Either[A, R]): Checked[A, Nothing, R] = either.fold(abort, result)

  /** Treat [[scala.Left$]] as non-abort with `default` as the result */
  def fromEitherNonabort[N, R](default: => R)(either: Either[N, R]): Checked[Nothing, N, R] =
    either.fold(continueWithResult(_, default), result)

  /** Treat [[scala.Left$]] as a chain of non-aborts with `default` as the result */
  def fromEitherNonaborts[N, R](default: => R)(
      either: Either[NonEmptyChain[N], R]
  ): Checked[Nothing, N, R] =
    either.fold(left => Result(left.toChain, default), result)

  /** Treat [[scala.Left$]] as abort */
  def fromEitherT[F[_], A, R](eitherT: EitherT[F, A, R])(implicit
      F: Functor[F]
  ): F[Checked[A, Nothing, R]] =
    F.map(eitherT.value)(fromEither)

  /** Treat [[scala.Left$]] as non-abort with `default` as the result */
  def fromEitherTNonabort[F[_], N, R](default: => R)(eitherT: EitherT[F, N, R])(implicit
      F: Functor[F]
  ): F[Checked[Nothing, N, R]] =
    F.map(eitherT.value)(fromEitherNonabort(default))

  /** Treat [[scala.Left$]] as a chain of non-aborts with `default` as the result */
  def fromEitherTNonaborts[F[_], N, R](default: => R)(eitherT: EitherT[F, NonEmptyChain[N], R])(
      implicit F: Functor[F]
  ): F[Checked[Nothing, N, R]] =
    F.map(eitherT.value)(fromEitherNonaborts(default))

  /** Treat test failure as abort */
  def cond[A, R](test: Boolean, right: => R, left: => A): Checked[A, Nothing, R] =
    if (test) Checked.result(right) else Checked.abort(left)

  implicit def cantonUtilMonadErrorForChecked[A, N]: MonadError[Checked[A, N, *], A] =
    new MonadError[Checked[A, N, *], A] {
      override def map[R, RR](fa: Checked[A, N, R])(f: R => RR): Checked[A, N, RR] = fa.map(f)

      override def pure[R](a: R): Checked[A, N, R] = Checked.result(a)

      override def ap[R, RR](ff: Checked[A, N, R => RR])(fa: Checked[A, N, R]): Checked[A, N, RR] =
        fa.ap(ff)

      override def product[R, RR](
          fa: Checked[A, N, R],
          fb: Checked[A, N, RR],
      ): Checked[A, N, (R, RR)] = fa.product(fb)

      override def map2Eval[R, RR, Z](fr: Checked[A, N, R], frr: Eval[Checked[A, N, RR]])(
          f: (R, RR) => Z
      ): Eval[Checked[A, N, Z]] = fr match {
        case abort @ Abort(_, _) => Now(abort)
        case Result(ns, r) => frr.map(_.prependNonaborts(ns).map(f(r, _)))
      }

      override def flatMap[R, RR](fa: Checked[A, N, R])(
          f: R => Checked[A, N, RR]
      ): Checked[A, N, RR] = fa.flatMap(f)

      override def tailRecM[S, R](s: S)(f: S => Checked[A, N, Either[S, R]]): Checked[A, N, R] = {
        @tailrec def go(old: Checked[A, N, Either[S, R]]): Checked[A, N, R] = old match {
          case abort @ Abort(_, _) => abort
          case Result(nonaborts, Left(state)) => go(f(state).prependNonaborts(nonaborts))
          case Result(nonaborts, Right(result)) => Result(nonaborts, result)
        }

        go(Checked.result(Left(s)))
      }

      override def raiseError[R](abort: A): Checked[A, N, R] = Checked.abort(abort)

      override def handleErrorWith[R](fa: Checked[A, N, R])(
          f: A => Checked[A, N, R]
      ): Checked[A, N, R] =
        fa.abortFlatMap(f)
    }
}
