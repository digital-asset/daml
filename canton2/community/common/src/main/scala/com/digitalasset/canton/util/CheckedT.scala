// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.{Chain, EitherT, Nested, NonEmptyChain, OptionT}
import cats.syntax.either.*
import cats.{Applicative, FlatMap, Functor, Monad, MonadError, Parallel, ~>}
import com.digitalasset.canton.FutureTransformer

/** Monad Transformer for [[Checked]], allowing the effect of a monad `F` to be combined with the aborting and
  * non-aborting failure effect of [[Checked]]. Similar to [[cats.data.EitherT]].
  */
@FutureTransformer(0)
final case class CheckedT[F[_], A, N, R](value: F[Checked[A, N, R]]) {

  import Checked.{Abort, Result}

  def map[RR](f: R => RR)(implicit F: Functor[F]): CheckedT[F, A, N, RR] = CheckedT(
    F.map(value)(_.map(f))
  )

  def mapAbort[AA](f: A => AA)(implicit F: Functor[F]): CheckedT[F, AA, N, R] = CheckedT(
    F.map(value)(_.mapAbort(f))
  )

  def mapNonaborts[NN](f: Chain[N] => Chain[NN])(implicit F: Functor[F]): CheckedT[F, A, NN, R] =
    CheckedT(F.map(value)(_.mapNonaborts(f)))

  def mapNonabort[NN](f: N => NN)(implicit F: Functor[F]): CheckedT[F, A, NN, R] =
    this.mapNonaborts(_.map(f))

  def trimap[AA, NN, RR](abortMap: A => AA, nonabortMap: N => NN, resultMap: R => RR)(implicit
      F: Functor[F]
  ): CheckedT[F, AA, NN, RR] =
    CheckedT(F.map(value)(_.trimap(abortMap, nonabortMap, resultMap)))

  def semiflatMap[RR](f: R => F[RR])(implicit F: Monad[F]): CheckedT[F, A, N, RR] =
    flatMap(result => CheckedT.result(f(result)))

  def fold[B](f: (A, Chain[N]) => B, g: (Chain[N], R) => B)(implicit F: Functor[F]): F[B] =
    F.map(value)(_.fold(f, g))

  def prependNonaborts[NN >: N](nonaborts: Chain[NN])(implicit
      F: Functor[F]
  ): CheckedT[F, A, NN, R] =
    mapNonaborts(Chain.concat(nonaborts, _))

  def prependNonabort[NN >: N](nonabort: NN)(implicit F: Functor[F]): CheckedT[F, A, NN, R] =
    prependNonaborts(Chain.one(nonabort))

  def appendNonaborts[NN >: N](nonaborts: Chain[NN])(implicit
      F: Functor[F]
  ): CheckedT[F, A, NN, R] =
    mapNonaborts(Chain.concat(_, nonaborts))

  def appendNonabort[NN >: N](nonabort: NN)(implicit F: Functor[F]): CheckedT[F, A, NN, R] =
    appendNonaborts(Chain.one(nonabort))

  /** Applicative product operation. Errors from `this` take precedence over `other` */
  def product[AA >: A, NN >: N, RR](other: CheckedT[F, AA, NN, RR])(implicit
      F: Applicative[F]
  ): CheckedT[F, AA, NN, (R, RR)] =
    CheckedT(F.map(F.product(this.value, other.value)) { case (x, y) => x.product(y) })

  /** Applicative operation. Consistent with [[flatMap]] according to Cats' laws.
    * Errors from the function take precedence over the function argument (=this).
    */
  def ap[AA >: A, NN >: N, RR](ff: CheckedT[F, AA, NN, R => RR])(implicit
      F: Applicative[F]
  ): CheckedT[F, AA, NN, RR] =
    CheckedT(F.map(F.product(ff.value, this.value)) { case (f, x) => x.ap(f) })

  def flatMap[AA >: A, NN >: N, RR](
      f: R => CheckedT[F, AA, NN, RR]
  )(implicit F: Monad[F]): CheckedT[F, AA, NN, RR] =
    CheckedT(F.flatMap(value) {
      case abort @ Abort(_, _) => F.pure(abort)
      case Result(nonaborts, result) => F.map(f(result).value)(_.prependNonaborts(nonaborts))
    })

  def biflatMap[AA, NN >: N, RR](f: A => CheckedT[F, AA, NN, RR], g: R => CheckedT[F, AA, NN, RR])(
      implicit F: FlatMap[F]
  ): CheckedT[F, AA, NN, RR] =
    CheckedT(F.flatMap(value) {
      case Abort(abort, nonaborts) => f(abort).prependNonaborts(nonaborts).value
      case Result(nonaborts, result) => g(result).prependNonaborts(nonaborts).value
    })

  def abortFlatMap[AA, NN >: N, RR >: R](f: A => CheckedT[F, AA, NN, RR])(implicit
      F: Monad[F]
  ): CheckedT[F, AA, NN, RR] =
    biflatMap(f, CheckedT.resultT(_))

  def subflatMap[AA >: A, NN >: N, RR](f: R => Checked[AA, NN, RR])(implicit
      F: Functor[F]
  ): CheckedT[F, AA, NN, RR] =
    CheckedT(F.map(value)(_.flatMap(f)))

  def abortSubflatMap[AA, NN >: N, RR >: R](
      f: A => Checked[AA, NN, RR]
  )(implicit F: Functor[F]): CheckedT[F, AA, NN, RR] = {
    CheckedT(F.map(value)(_.abortFlatMap(f)))
  }

  /** Merges aborts with nonaborts, using the given `default` result if no result is contained. */
  def toResult[NN, A1 >: A <: NN, N1 >: N <: NN](default: => R)(implicit
      F: Functor[F]
  ): CheckedT[F, Nothing, NN, R] =
    CheckedT[F, Nothing, NN, R](F.map(value)(_.toResult[NN, R, A1, N1](default)))

  /** Flatmap if the Checked is successful, otherwise return the current result value. */
  def flatMapIfSuccess[RR >: R, AA >: A, NN >: N](
      f: R => CheckedT[F, AA, NN, RR]
  )(implicit F: Monad[F]): CheckedT[F, AA, NN, RR] =
    CheckedT(F.flatMap(value) {
      case abort @ Abort(_, _) => F.pure(abort)
      case r @ Result(nonaborts, result) =>
        if (nonaborts.isEmpty)
          f(result).value
        else F.pure(r)
    })

  def foreach(f: R => Unit)(implicit F: Functor[F]): F[Unit] = F.map(value)(_.foreach(f))
  def exists(pred: R => Boolean)(implicit F: Functor[F]): F[Boolean] = F.map(value)(_.exists(pred))
  def forall(pred: R => Boolean)(implicit F: Functor[F]): F[Boolean] = F.map(value)(_.forall(pred))

  /** Discards nonaborts. */
  def toEitherT(implicit F: Functor[F]): EitherT[F, A, R] = EitherT(F.map(value)(_.toEither))

  /** Discards results if there are nonaborts. */
  def toEitherTWithNonaborts[L, A1 >: A <: L, N1 >: N <: L](implicit
      F: Functor[F]
  ): EitherT[F, NonEmptyChain[L], R] =
    EitherT(F.map(value)(_.toEitherWithNonaborts[L, A1, N1]))

  def toOptionT(implicit F: Functor[F]): OptionT[F, R] = OptionT(F.map(value)(_.toOption))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def widenResult[RR >: R]: CheckedT[F, A, N, RR] = this.asInstanceOf[CheckedT[F, A, N, RR]]

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def widenAbort[AA >: A]: CheckedT[F, AA, N, R] = this.asInstanceOf[CheckedT[F, AA, N, R]]

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def widenNonabort[NN >: N]: CheckedT[F, A, NN, R] = this.asInstanceOf[CheckedT[F, A, NN, R]]
}

object CheckedT extends CheckedTInstances {

  def abort[N, R]: AbortPartiallyApplied[N, R] = new AbortPartiallyApplied[N, R]

  /** Uses the [[http://typelevel.org/cats/guidelines.html#partially-applied-type-params Partially Applied Type Params technique]]
    * for ergonomics.
    */
  final private[util] class AbortPartiallyApplied[N, R](private val dummy: Boolean = true)
      extends AnyVal {
    def apply[F[_], A](abort: F[A])(implicit F: Functor[F]): CheckedT[F, A, N, R] =
      CheckedT(F.map(abort)(Checked.abort))
  }

  def abortT[F[_], N, R]: AbortTPartiallyApplied[F, N, R] = new AbortTPartiallyApplied[F, N, R]
  final private[util] class AbortTPartiallyApplied[F[_], N, R](private val dummy: Boolean = true)
      extends AnyVal {
    def apply[A](abort: A)(implicit F: Applicative[F]): CheckedT[F, A, N, R] =
      CheckedT(F.pure(Checked.abort(abort)))
  }

  def result[A, N]: ResultPartiallyApplied[A, N] = new ResultPartiallyApplied[A, N]
  final private[util] class ResultPartiallyApplied[A, N](private val dummy: Boolean = true)
      extends AnyVal {
    def apply[F[_], R](result: F[R])(implicit F: Functor[F]): CheckedT[F, A, N, R] =
      CheckedT(F.map(result)(Checked.result))
  }

  def resultT[F[_], A, N]: ResultTPartiallyApplied[F, A, N] = new ResultTPartiallyApplied[F, A, N]
  final private[util] class ResultTPartiallyApplied[F[_], A, N](private val dummy: Boolean = true)
      extends AnyVal {
    def apply[R](result: R)(implicit F: Applicative[F]): CheckedT[F, A, N, R] =
      CheckedT(F.pure(Checked.result(result)))
  }
  def pure[F[_], A, N]: ResultTPartiallyApplied[F, A, N] = resultT

  def continueWithResultT[F[_], A]: ContinueWithResultTPartiallyApplied[F, A] =
    new ContinueWithResultTPartiallyApplied[F, A]
  final private[util] class ContinueWithResultTPartiallyApplied[F[_], A](
      private val dummy: Boolean = true
  ) extends AnyVal {
    def apply[N, R](nonabort: N, result: R)(implicit F: Applicative[F]): CheckedT[F, A, N, R] =
      CheckedT(F.pure(Checked.continueWithResult(nonabort, result)))
  }

  def continue[A]: ContinuePartiallyApplied[A] = new ContinuePartiallyApplied[A]
  final private[util] class ContinuePartiallyApplied[A](private val dummy: Boolean = true)
      extends AnyVal {
    def apply[F[_], N](nonabort: F[N])(implicit F: Applicative[F]): CheckedT[F, A, N, Unit] =
      CheckedT(F.map(nonabort)(Checked.continue))
  }

  def continueT[F[_], A]: ContinueTPartiallyApplied[F, A] = new ContinueTPartiallyApplied[F, A]
  final private[util] class ContinueTPartiallyApplied[F[_], A](private val dummy: Boolean = true)
      extends AnyVal {
    def apply[N](nonabort: N)(implicit F: Applicative[F]): CheckedT[F, A, N, Unit] =
      CheckedT(F.pure(Checked.continue(nonabort)))
  }

  def fromChecked[F[_]]: FromCheckedPartiallyApplied[F] = new FromCheckedPartiallyApplied[F]
  final private[util] class FromCheckedPartiallyApplied[F[_]](private val dummy: Boolean = true)
      extends AnyVal {
    def apply[A, N, R](checked: Checked[A, N, R])(implicit
        F: Applicative[F]
    ): CheckedT[F, A, N, R] =
      CheckedT(F.pure(checked))
  }

  /** Treat [[scala.Left$]] as abort */
  def fromEitherT[N]: FromEitherTPartiallyApplied[N] = new FromEitherTPartiallyApplied[N]
  final private[util] class FromEitherTPartiallyApplied[N](private val dummy: Boolean = true)
      extends AnyVal {
    def apply[F[_], A, R](eitherT: EitherT[F, A, R])(implicit F: Functor[F]): CheckedT[F, A, N, R] =
      CheckedT(F.map(eitherT.value)(Checked.fromEither))
  }

  /** Treat [[scala.Left$]] as non-abort with `default` as the result */
  def fromEitherTNonabort[A]: FromEitherTNonabortPartiallyApplied[A] =
    new FromEitherTNonabortPartiallyApplied[A]
  final private[util] class FromEitherTNonabortPartiallyApplied[A](
      private val dummy: Boolean = true
  ) extends AnyVal {
    def apply[F[_], N, R](default: => R, eitherT: EitherT[F, N, R])(implicit
        F: Functor[F]
    ): CheckedT[F, A, N, R] =
      CheckedT(F.map(eitherT.value)(Checked.fromEitherNonabort(default)))
  }
}

trait CheckedTInstances extends CheckedTInstances1 {

  implicit def cantonUtilMonadErrorForCheckedT[F[_], A, N](implicit
      F0: Monad[F]
  ): MonadError[CheckedT[F, A, N, *], A] =
    new CheckedTMonadError[F, A, N] {
      implicit val F = F0
    }

  implicit def cantonUtilParallelForCheckedT[M[_], A, N](implicit
      P: Parallel[M]
  ): Parallel.Aux[CheckedT[M, A, N, *], Nested[P.F, Checked[A, N, *], *]] =
    new Parallel[CheckedT[M, A, N, *]] {
      type F[x] = Nested[P.F, Checked[A, N, *], x]

      implicit val monadEither: Monad[Checked[A, N, *]] =
        Checked.cantonUtilMonadErrorForChecked

      def applicative: Applicative[Nested[P.F, Checked[A, N, *], *]] =
        cats.data.Nested.catsDataApplicativeForNested(P.applicative, implicitly)

      def monad: Monad[CheckedT[M, A, N, *]] = CheckedT.cantonUtilMonadErrorForCheckedT(P.monad)

      def sequential: Nested[P.F, Checked[A, N, *], *] ~> CheckedT[M, A, N, *] =
        new (Nested[P.F, Checked[A, N, *], *] ~> CheckedT[M, A, N, *]) {
          def apply[R](nested: Nested[P.F, Checked[A, N, *], R]): CheckedT[M, A, N, R] = {
            val mva = P.sequential(nested.value)
            CheckedT(mva)
          }
        }

      def parallel: CheckedT[M, A, N, *] ~> Nested[P.F, Checked[A, N, *], *] =
        new (CheckedT[M, A, N, *] ~> Nested[P.F, Checked[A, N, *], *]) {
          def apply[R](checkedT: CheckedT[M, A, N, R]): Nested[P.F, Checked[A, N, *], R] = {
            val fea = P.parallel(checkedT.value)
            Nested(fea)
          }
        }
    }
}

trait CheckedTInstances1 extends CheckedTInstances2 {
  implicit def cantonUtilApplicativeForCheckedT[F[_], A, N](implicit
      F0: Applicative[F]
  ): Applicative[CheckedT[F, A, N, *]] =
    new CheckedTApplicative[F, A, N] {
      implicit val F = F0
    }
}

trait CheckedTInstances2 {
  implicit def cantonUtilFunctorForCheckedT[F[_], A, N](implicit
      F0: Functor[F]
  ): Functor[CheckedT[F, A, N, *]] =
    new CheckedTFunctor[F, A, N] {
      implicit val F = F0
    }
}

private[util] trait CheckedTFunctor[F[_], A, N] extends Functor[CheckedT[F, A, N, *]] {
  implicit val F: Functor[F]
  override def map[R, RR](checkedT: CheckedT[F, A, N, R])(f: R => RR): CheckedT[F, A, N, RR] =
    checkedT.map(f)
}

private[util] trait CheckedTApplicative[F[_], A, N] extends Applicative[CheckedT[F, A, N, *]] {
  implicit val F: Applicative[F]

  override def pure[R](x: R): CheckedT[F, A, N, R] = CheckedT(F.pure(Checked.result(x)))

  override def ap[R, S](ff: CheckedT[F, A, N, R => S])(
      fa: CheckedT[F, A, N, R]
  ): CheckedT[F, A, N, S] = fa.ap(ff)
}

private[util] trait CheckedTMonadError[F[_], A, N]
    extends MonadError[CheckedT[F, A, N, *], A]
    with CheckedTFunctor[F, A, N] {
  implicit val F: Monad[F]
  override def pure[R](result: R): CheckedT[F, A, N, R] = CheckedT.pure(result)

  override def flatMap[R, RR](x: CheckedT[F, A, N, R])(
      f: R => CheckedT[F, A, N, RR]
  ): CheckedT[F, A, N, RR] =
    x.flatMap(f)

  override def tailRecM[S, R](
      x: S
  )(f: S => CheckedT[F, A, N, Either[S, R]]): CheckedT[F, A, N, R] = {
    def step: ((Chain[N], S)) => F[Either[(Chain[N], S), Checked[A, N, R]]] = {
      case (nonaborts1, s) =>
        import Checked.{Abort, Result}
        F.map(f(s).value) {
          case abort @ Abort(_, _) => Right(abort.prependNonaborts(nonaborts1))
          case Result(nonaborts2, sr) =>
            val nonaborts = Chain.concat(nonaborts1, nonaborts2)
            sr.bimap(l => (nonaborts, l), r => Result(nonaborts, r))
        }
    }

    CheckedT(F.tailRecM((Chain.empty[N], x))(step))
  }

  override def raiseError[R](abort: A): CheckedT[F, A, N, R] = CheckedT.abortT(abort)

  override def handleErrorWith[R](fa: CheckedT[F, A, N, R])(
      f: A => CheckedT[F, A, N, R]
  ): CheckedT[F, A, N, R] =
    fa.abortFlatMap(f)
}
