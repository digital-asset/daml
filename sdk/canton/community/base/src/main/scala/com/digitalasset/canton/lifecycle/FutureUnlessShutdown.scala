// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.arrow.FunctionK
import cats.data.EitherT
import cats.{Applicative, FlatMap, Functor, Id, Monad, MonadThrow, Monoid, Parallel, ~>}
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle.Timer
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.AbortedDueToShutdownException
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{LoggerUtil, Thereafter, ThereafterAsync}
import com.digitalasset.canton.{
  DoNotDiscardLikeFuture,
  DoNotReturnFromSynchronizedLikeFuture,
  DoNotTraverseLikeFuture,
}

import scala.concurrent.{Awaitable, ExecutionContext, Future}
import scala.util.chaining.*
import scala.util.{Failure, Success, Try}

object FutureUnlessShutdown {

  /** Close the type abstraction of [[FutureUnlessShutdown]] */
  def apply[A](x: Future[UnlessShutdown[A]]): FutureUnlessShutdown[A] = {
    type K[T[_]] = Id[T[A]]
    FutureUnlessShutdownImpl.Instance.subst[K](x)
  }

  /** Immediately report [[UnlessShutdown.AbortedDueToShutdown]] */
  val abortedDueToShutdown: FutureUnlessShutdown[Nothing] =
    FutureUnlessShutdown(Future.successful(UnlessShutdown.AbortedDueToShutdown))

  /** Analog to [[scala.concurrent.Future]]`.unit` */
  val unit: FutureUnlessShutdown[Unit] = FutureUnlessShutdown(
    Future.successful(UnlessShutdown.unit)
  )

  /** Analog to [[scala.concurrent.Future]]`.successful` */
  def pure[A](x: A): FutureUnlessShutdown[A] = lift(UnlessShutdown.Outcome(x))

  def lift[A](x: UnlessShutdown[A]): FutureUnlessShutdown[A] = FutureUnlessShutdown(
    Future.successful(x)
  )

  /** Wraps the result of a [[scala.concurrent.Future]] into an [[UnlessShutdown.Outcome]] */
  def outcomeF[A](f: Future[A])(implicit ec: ExecutionContext): FutureUnlessShutdown[A] =
    FutureUnlessShutdown(f.map(UnlessShutdown.Outcome(_)))

  /** [[outcomeF]] as a [[cats.arrow.FunctionK]] to be used with Cat's `mapK` operation.
    *
    * Can be used to switch from [[scala.concurrent.Future]] to [[FutureUnlessShutdown]] inside another
    * functor/applicative/monad such as [[cats.data.EitherT]] via `eitherT.mapK(outcomeK)`.
    */
  def outcomeK(implicit ec: ExecutionContext): Future ~> FutureUnlessShutdown =
    // We can't use `FunctionK.lift` here because of the implicit execution context.
    new FunctionK[Future, FutureUnlessShutdown] {
      override def apply[A](future: Future[A]): FutureUnlessShutdown[A] = outcomeF(future)
    }

  def liftK: UnlessShutdown ~> FutureUnlessShutdown = FunctionK.lift(lift)

  /** Analog to [[scala.concurrent.Future]]`.failed` */
  def failed[A](ex: Throwable): FutureUnlessShutdown[A] = FutureUnlessShutdown(Future.failed(ex))

  /** Analog to [[scala.concurrent.Future]]`.fromTry` */
  def fromTry[T](result: Try[T]): FutureUnlessShutdown[T] = result match {
    case Success(value) => FutureUnlessShutdown.pure(value)
    case Failure(exception) => FutureUnlessShutdown.failed(exception)
  }

  def never: FutureUnlessShutdown[Nothing] = FutureUnlessShutdown(Future.never)

  /** Can transform a future from [[FutureUnlessShutdownImpl.Ops.failOnShutdownToAbortException]] back
    * to [[FutureUnlessShutdown]].
    */
  def transformAbortedF[V](f: Future[V])(implicit ec: ExecutionContext): FutureUnlessShutdown[V] =
    apply(f.transform({
      case Success(value) => Success(UnlessShutdown.Outcome(value))
      case Failure(AbortedDueToShutdownException(_)) => Success(UnlessShutdown.AbortedDueToShutdown)
      case Failure(other) => Failure(other)
    }))
}

/** Monad combination of `Future` and [[UnlessShutdown]]
  *
  * We avoid wrapping and unwrapping it by emulating Scala 3's opaque types.
  * This makes the asynchronous detection magic work out of the box for [[FutureUnlessShutdown]]
  * because `FutureUnlessShutdown(x).isInstanceOf[Future]` holds at runtime.
  */
sealed abstract class FutureUnlessShutdownImpl {

  /** The abstract type of a [[scala.concurrent.Future]] containing a [[UnlessShutdown]].
    * We can't make it a subtype of [[scala.concurrent.Future]]`[`[[UnlessShutdown]]`]` itself
    * because we want to change the signature and implementation of some methods like [[scala.concurrent.Future.flatMap]].
    * So [[FutureUnlessShutdown]] up-casts only into an [[scala.concurrent.Awaitable]].
    *
    * The canonical name for this type would be `T`, but `FutureUnlessShutdown` gives better error messages.
    */
  @DoNotDiscardLikeFuture
  @DoNotTraverseLikeFuture
  @DoNotReturnFromSynchronizedLikeFuture
  type FutureUnlessShutdown[+A] <: Awaitable[UnlessShutdown[A]]

  /** Methods to evidence that [[FutureUnlessShutdown]] and [[scala.concurrent.Future]]`[`[[UnlessShutdown]]`]`
    * can be replaced in any type context `K`.
    */
  private[lifecycle] def subst[K[_[_]]](
      ff: K[Lambda[a => Future[UnlessShutdown[a]]]]
  ): K[FutureUnlessShutdown]
  // Technically, we could implement `unsubst` using `subst`, but it may be clearer if we make both directions explicit.
  private[lifecycle] def unsubst[K[_[_]]](
      ff: K[FutureUnlessShutdown]
  ): K[Lambda[a => Future[UnlessShutdown[a]]]]
}

object FutureUnlessShutdownImpl {
  val Instance: FutureUnlessShutdownImpl = new FutureUnlessShutdownImpl {
    override type FutureUnlessShutdown[+A] = Future[UnlessShutdown[A]]

    override private[lifecycle] def subst[F[_[_]]](
        ff: F[Lambda[a => Future[UnlessShutdown[a]]]]
    ): F[FutureUnlessShutdown] = ff
    override private[lifecycle] def unsubst[F[_[_]]](
        ff: F[FutureUnlessShutdown]
    ): F[Lambda[a => Future[UnlessShutdown[a]]]] = ff
  }

  /** Extension methods for [[FutureUnlessShutdown]] */
  implicit final class Ops[+A](private val self: FutureUnlessShutdown[A]) extends AnyVal {

    /** Open the type abstraction */
    def unwrap: Future[UnlessShutdown[A]] = {
      type K[T[_]] = Id[T[A]]
      Instance.unsubst[K](self)
    }

    /** Analog to [[scala.concurrent.Future]].`transform` */
    def transform[B](f: Try[UnlessShutdown[A]] => Try[UnlessShutdown[B]])(implicit
        ec: ExecutionContext
    ): FutureUnlessShutdown[B] =
      FutureUnlessShutdown(unwrap.transform(f))

    def transformIntoSuccess[B](f: Try[UnlessShutdown[A]] => UnlessShutdown[B])(implicit
        ec: ExecutionContext
    ): FutureUnlessShutdown[B] =
      transform(x => Success(f(x)))

    /** Analog to [[scala.concurrent.Future]].`transform` */
    def transform[B](
        success: UnlessShutdown[A] => UnlessShutdown[B],
        failure: Throwable => Throwable,
    )(implicit ec: ExecutionContext): FutureUnlessShutdown[B] =
      FutureUnlessShutdown(unwrap.transform(success, failure))

    /** Analog to [[scala.concurrent.Future.transformWith]] */
    def transformWith[B](
        f: Try[UnlessShutdown[A]] => FutureUnlessShutdown[B]
    )(implicit ec: ExecutionContext): FutureUnlessShutdown[B] = {
      type K[F[_]] = Try[UnlessShutdown[A]] => F[B]
      FutureUnlessShutdown(unwrap.transformWith(Instance.unsubst[K](f)))
    }

    def andThen[B](pf: PartialFunction[Try[UnlessShutdown[A]], B])(implicit
        executor: ExecutionContext
    ): FutureUnlessShutdown[A] =
      FutureUnlessShutdown(unwrap.andThen(pf))

    /** Analog to [[scala.concurrent.Future]].onComplete */
    def onComplete[B](f: Try[UnlessShutdown[A]] => Unit)(implicit ec: ExecutionContext): Unit =
      unwrap.onComplete(f)

    /** Analog to [[scala.concurrent.Future]].`failed` */
    def failed(implicit ec: ExecutionContext): FutureUnlessShutdown[Throwable] =
      FutureUnlessShutdown.outcomeF(self.unwrap.failed)

    /** Evaluates `f` and returns its result if this future completes with [[UnlessShutdown.AbortedDueToShutdown]]. */
    def onShutdown[B >: A](f: => B)(implicit ec: ExecutionContext): Future[B] =
      unwrap.map(_.onShutdown(f))

    def failOnShutdownToAbortException(action: String)(implicit ec: ExecutionContext): Future[A] =
      failOnShutdownTo(AbortedDueToShutdownException(action))

    /** consider using [[failOnShutdownToAbortException]] unless you need a specific exception. */
    def failOnShutdownTo(t: => Throwable)(implicit ec: ExecutionContext): Future[A] = {
      unwrap.flatMap {
        case UnlessShutdown.Outcome(result) => Future.successful(result)
        case UnlessShutdown.AbortedDueToShutdown => Future.failed(t)
      }
    }

    def isCompleted: Boolean = {
      unwrap.isCompleted
    }

    /** Evaluates `f` on shutdown but retains the result of the future. */
    def tapOnShutdown(f: => Unit)(implicit
        ec: ExecutionContext,
        errorLoggingContext: ErrorLoggingContext,
    ): FutureUnlessShutdown[A] = FutureUnlessShutdown {
      import Thereafter.syntax.*
      this.unwrap.thereafter {
        case Success(UnlessShutdown.AbortedDueToShutdown) => LoggerUtil.logOnThrow(f)
        case _ =>
      }
    }

    // This method is here so that we don't need to import ```cats.syntax.flatmap._``` everywhere
    def flatMap[B](f: A => FutureUnlessShutdown[B])(implicit
        ec: ExecutionContext
    ): FutureUnlessShutdown[B] =
      FlatMap[FutureUnlessShutdown].flatMap(self)(f)

    // This method is here so that we don't need to import ```cats.syntax.functor._``` everywhere
    def map[B](f: A => B)(implicit ec: ExecutionContext): FutureUnlessShutdown[B] =
      Functor[FutureUnlessShutdown].map(self)(f)

    def subflatMap[B](f: A => UnlessShutdown[B])(implicit
        ec: ExecutionContext
    ): FutureUnlessShutdown[B] =
      FutureUnlessShutdown(self.unwrap.map(_.flatMap(f)))

    def flatten[S](implicit
        ec: ExecutionContext,
        ev: A <:< FutureUnlessShutdown[S],
    ): FutureUnlessShutdown[S] =
      self.flatMap(ev)

    /** Analog to [[scala.concurrent.Future]].recover */
    def recover[U >: A](
        pf: PartialFunction[Throwable, UnlessShutdown[U]]
    )(implicit executor: ExecutionContext): FutureUnlessShutdown[U] =
      transform[U] { (value: Try[UnlessShutdown[A]]) =>
        value recover pf
      }
  }

  /** Cats monad instance for the combination of [[scala.concurrent.Future]] with [[UnlessShutdown]].
    * [[UnlessShutdown.AbortedDueToShutdown]] short-circuits sequencing.
    */
  private def monadFutureUnlessShutdownOpened(implicit
      ec: ExecutionContext
  ): MonadThrow[λ[α => Future[UnlessShutdown[α]]]] =
    new MonadThrow[λ[α => Future[UnlessShutdown[α]]]] {
      override def pure[A](x: A): Future[UnlessShutdown[A]] =
        Future.successful(UnlessShutdown.Outcome(x))

      override def flatMap[A, B](
          a: Future[UnlessShutdown[A]]
      )(f: A => Future[UnlessShutdown[B]]): Future[UnlessShutdown[B]] =
        a.flatMap {
          case UnlessShutdown.Outcome(x) => f(x)
          case UnlessShutdown.AbortedDueToShutdown =>
            Future.successful(UnlessShutdown.AbortedDueToShutdown)
        }

      override def tailRecM[A, B](
          a: A
      )(f: A => Future[UnlessShutdown[Either[A, B]]]): Future[UnlessShutdown[B]] =
        Monad[Future].tailRecM(a)(a0 =>
          f(a0).map {
            case UnlessShutdown.AbortedDueToShutdown => Right(UnlessShutdown.AbortedDueToShutdown)
            case UnlessShutdown.Outcome(Left(a1)) => Left(a1)
            case UnlessShutdown.Outcome(Right(b)) => Right(UnlessShutdown.Outcome(b))
          }
        )

      override def raiseError[A](e: Throwable): Future[UnlessShutdown[A]] = Future.failed(e)

      override def handleErrorWith[A](
          fa: Future[UnlessShutdown[A]]
      )(f: Throwable => Future[UnlessShutdown[A]]): Future[UnlessShutdown[A]] = {
        fa.recoverWith { case throwable => f(throwable) }
      }
    }

  implicit def catsStdInstFutureUnlessShutdown(implicit
      ec: ExecutionContext
  ): MonadThrow[FutureUnlessShutdown] =
    Instance.subst[MonadThrow](monadFutureUnlessShutdownOpened)

  implicit def monoidFutureUnlessShutdown[A](implicit
      M: Monoid[A],
      ec: ExecutionContext,
  ): Monoid[FutureUnlessShutdown[A]] = {
    type K[T[_]] = Monoid[T[A]]
    Instance.subst[K](Monoid[Future[UnlessShutdown[A]]])
  }

  private def parallelApplicativeFutureUnlessShutdownOpened(implicit
      ec: ExecutionContext
  ): Applicative[Lambda[alpha => Future[UnlessShutdown[alpha]]]] =
    new Applicative[Lambda[alpha => Future[UnlessShutdown[alpha]]]] {
      private val applicativeUnlessShutdown = Applicative[UnlessShutdown]

      override def pure[A](x: A): Future[UnlessShutdown[A]] =
        Future.successful(UnlessShutdown.Outcome(x))

      override def ap[A, B](ff: Future[UnlessShutdown[A => B]])(
          fa: Future[UnlessShutdown[A]]
      ): Future[UnlessShutdown[B]] = ff.zipWith(fa)((f, a) => applicativeUnlessShutdown.ap(f)(a))
    }

  def parallelApplicativeFutureUnlessShutdown(implicit
      ec: ExecutionContext
  ): Applicative[FutureUnlessShutdown] =
    Instance.subst[Applicative](parallelApplicativeFutureUnlessShutdownOpened)

  private def parallelInstanceFutureUnlessShutdownOpened(implicit
      ec: ExecutionContext
  ): Parallel[Lambda[alpha => Future[UnlessShutdown[alpha]]]] =
    new Parallel[Lambda[alpha => Future[UnlessShutdown[alpha]]]] {
      override type F[X] = Future[UnlessShutdown[X]]

      override def applicative: Applicative[F] = parallelApplicativeFutureUnlessShutdownOpened

      override def monad: Monad[Lambda[alpha => Future[UnlessShutdown[alpha]]]] =
        monadFutureUnlessShutdownOpened

      override def sequential: F ~> Lambda[alpha => Future[UnlessShutdown[alpha]]] = FunctionK.id

      override def parallel: Lambda[alpha => Future[UnlessShutdown[alpha]]] ~> F = FunctionK.id
    }

  implicit def parallelInstanceFutureUnlessShutdown(implicit
      ec: ExecutionContext
  ): Parallel[FutureUnlessShutdown] =
    Instance.subst[Parallel](parallelInstanceFutureUnlessShutdownOpened)

  class FutureUnlessShutdownThereafter(implicit ec: ExecutionContext)
      extends ThereafterAsync[FutureUnlessShutdown] {
    override type Content[A] = FutureUnlessShutdownThereafterContent[A]
    override def thereafter[A](f: FutureUnlessShutdown[A])(
        body: Try[UnlessShutdown[A]] => Unit
    ): FutureUnlessShutdown[A] =
      FutureUnlessShutdown(f.unwrap.thereafter(body))

    override def thereafterF[A](f: FutureUnlessShutdown[A])(
        body: Try[UnlessShutdown[A]] => Future[Unit]
    ): FutureUnlessShutdown[A] = {
      FutureUnlessShutdown(ThereafterAsync[Future].thereafterF(f.unwrap)(body))
    }

    override def maybeContent[A](content: FutureUnlessShutdownThereafterContent[A]): Option[A] =
      content match {
        case Success(UnlessShutdown.Outcome(x)) => Some(x)
        case _ => None
      }
  }

  /** Use a type synonym instead of a type lambda so that the Scala compiler does not get confused during implicit resolution,
    * at least for simple cases.
    */
  type FutureUnlessShutdownThereafterContent[A] = Try[UnlessShutdown[A]]
  implicit def thereafterFutureUnlessShutdown(implicit
      ec: ExecutionContext
  ): ThereafterAsync.Aux[FutureUnlessShutdown, FutureUnlessShutdownThereafterContent] =
    new FutureUnlessShutdownThereafter

  /** Enable `onShutdown` syntax on [[cats.data.EitherT]]`[`[[FutureUnlessShutdown]]`...]`. */
  implicit class EitherTOnShutdownSyntax[A, B](
      private val eitherT: EitherT[FutureUnlessShutdown, A, B]
  ) extends AnyVal {
    def failOnShutdownTo[C >: A, D >: B](t: => Throwable)(implicit
        ec: ExecutionContext
    ): EitherT[Future, C, D] =
      EitherT(eitherT.value.failOnShutdownTo(t))

    def onShutdown[C >: A, D >: B](f: => Either[C, D])(implicit
        ec: ExecutionContext
    ): EitherT[Future, C, D] =
      EitherT(eitherT.value.onShutdown(f))

    def tapLeft(f: A => Unit)(implicit
        ec: ExecutionContext
    ): EitherT[FutureUnlessShutdown, A, B] = eitherT.leftMap(_.tap(f))

    /** Evaluates `f` on shutdown but retains the result of the future. */
    def tapOnShutdown(f: => Unit)(implicit
        ec: ExecutionContext,
        errorLoggingContext: ErrorLoggingContext,
    ): EitherT[FutureUnlessShutdown, A, B] =
      EitherT(eitherT.value.tapOnShutdown(f))
  }

  implicit class TimerOnShutdownSyntax(private val timed: Timed.type) extends AnyVal {
    def future[T](timer: Timer, future: => FutureUnlessShutdown[T]): FutureUnlessShutdown[T] =
      FutureUnlessShutdown(timed.future(timer, future.unwrap))
  }

  final case class AbortedDueToShutdownException(action: String)
      extends RuntimeException(s"'$action' was aborted due to shutdown.")

}
