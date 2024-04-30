// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import cats.syntax.either.*
import cats.{Applicative, Functor}
import com.daml.metrics.api.MetricHandle.Timer
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Utility functions for the `cats ` [[cats.data.EitherT]] monad transformer.
  * https://typelevel.org/cats/datatypes/eithert.html
  */
object EitherTUtil {

  /** Similar to `finallyET` but will only call the provided handler if `fn` returns a left/error or fails. */
  def onErrorOrFailure[A, B](errorHandler: () => Unit)(
      fn: => EitherT[Future, A, B]
  )(implicit executionContext: ExecutionContext): EitherT[Future, A, B] =
    fn.thereafterSuccessOrFailure(_ => (), errorHandler())

  def onErrorOrFailureUnlessShutdown[A, B](
      errorHandler: Either[Throwable, A] => Unit,
      shutdownHandler: () => Unit = () => (),
  )(
      fn: => EitherT[FutureUnlessShutdown, A, B]
  )(implicit executionContext: ExecutionContext): EitherT[FutureUnlessShutdown, A, B] =
    fn.thereafter {
      case Failure(t) =>
        errorHandler(Left(t))
      case Success(UnlessShutdown.Outcome(Left(left))) =>
        errorHandler(Right(left))
      case Success(UnlessShutdown.AbortedDueToShutdown) =>
        shutdownHandler()
      case _ => ()
    }

  /** Lifts an `if (cond) then ... else ()` into the `EitherT` a  pplicative */
  def ifThenET[F[_], L](cond: Boolean)(`then`: => EitherT[F, L, _])(implicit
      F: Applicative[F]
  ): EitherT[F, L, Unit] =
    if (cond) Functor[EitherT[F, L, *]].void(`then`) else EitherT.pure[F, L](())

  def condUnitET[F[_]]: CondUnitEitherTPartiallyApplied[F] =
    new CondUnitEitherTPartiallyApplied[F]()
  private[util] final class CondUnitEitherTPartiallyApplied[F[_]](private val dummy: Boolean = true)
      extends AnyVal {
    def apply[L](condition: Boolean, fail: => L)(implicit F: Applicative[F]): EitherT[F, L, Unit] =
      EitherT.cond[F](condition, (), fail)
  }

  def leftSubflatMap[F[_], A, B, C, BB >: B](x: EitherT[F, A, B])(f: A => Either[C, BB])(implicit
      F: Functor[F]
  ): EitherT[F, C, BB] =
    EitherT(F.map(x.value)(_.leftFlatMap(f)))

  /** Construct an EitherT from a possibly failed future. */
  def fromFuture[E, A](fut: Future[A], errorHandler: Throwable => E)(implicit
      ec: ExecutionContext
  ): EitherT[Future, E, A] =
    liftFailedFuture(fut.map(Right(_)), errorHandler)

  /** Variation of fromFuture  that takes a [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] */
  def fromFuture[E, A](futUnlSht: FutureUnlessShutdown[A], errorHandler: Throwable => E)(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, E, A] =
    liftFailedFuture(futUnlSht.map(Right(_)), errorHandler)

  /** Lift a failed future into a Left value. */
  def liftFailedFuture[E, A](fut: Future[Either[E, A]], errorHandler: Throwable => E)(implicit
      executionContext: ExecutionContext
  ): EitherT[Future, E, A] =
    EitherT(fut.recover[Either[E, A]] { case NonFatal(x) =>
      errorHandler(x).asLeft[A]
    })

  /** Variation of liftFailedFuture that takes a [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] */
  def liftFailedFuture[E, A](
      futUnlSht: FutureUnlessShutdown[Either[E, A]],
      errorHandler: Throwable => E,
  )(implicit
      executionContext: ExecutionContext
  ): EitherT[FutureUnlessShutdown, E, A] = {
    EitherT(futUnlSht.recover[Either[E, A]] { case NonFatal(x) =>
      UnlessShutdown.Outcome(errorHandler(x).asLeft[A])
    })
  }

  /** Log `message` if `result` fails with an exception or results in a `Left` */
  def logOnError[E, R](result: EitherT[Future, E, R], message: String, level: Level = Level.ERROR)(
      implicit
      executionContext: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): EitherT[Future, E, R] = {

    def logError(v: Try[Either[E, R]]): Unit =
      v match {
        case Success(Left(err)) => LoggerUtil.logAtLevel(level, message + " " + err.toString)
        case Failure(NonFatal(err)) => LoggerUtil.logThrowableAtLevel(level, message, err)
        case _ => ()
      }

    result.thereafter(logError)
  }

  /** Log `message` if `result` fails with an exception or results in a `Left` */
  def logOnErrorU[E, R](
      result: EitherT[FutureUnlessShutdown, E, R],
      message: String,
      level: Level = Level.ERROR,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, E, R] = {

    def logError(v: Try[UnlessShutdown[Either[E, R]]]): Unit =
      v match {
        case Success(Outcome(Left(err))) =>
          LoggerUtil.logAtLevel(level, message + " " + err.toString)
        case Failure(NonFatal(err)) => LoggerUtil.logThrowableAtLevel(level, message, err)
        case _ => ()
      }

    result.thereafter(logError)
  }

  /** Discard `eitherT` and log an error if it does not result in a `Right`.
    * This is useful to document that an `EitherT[Future,_,_]` is intentionally not being awaited upon.
    */
  def doNotAwait(
      eitherT: EitherT[Future, _, _],
      failureMessage: => String,
      level: Level = Level.ERROR,
  )(implicit executionContext: ExecutionContext, loggingContext: ErrorLoggingContext): Unit =
    logOnError(eitherT, failureMessage, level = level).discard

  def doNotAwaitUS(
      eitherT: EitherT[FutureUnlessShutdown, _, _],
      message: => String,
      failLevel: Level = Level.ERROR,
      shutdownLevel: Level = Level.DEBUG,
  )(implicit executionContext: ExecutionContext, loggingContext: ErrorLoggingContext): Unit = {
    val failureMessage = s"$message failed"
    val shutdownMessage = s"$message aborted due to shutdown"
    logOnErrorU(eitherT, failureMessage, level = failLevel).value
      .map(_ => ())
      .onShutdown(LoggerUtil.logAtLevel(shutdownLevel, shutdownMessage))
      .discard
  }

  /** Measure time of EitherT-based calls, inspired by upstream com.daml.metrics.Timed.future */
  def timed[F[_], E, R](timerMetric: Timer)(
      code: => EitherT[F, E, R]
  )(implicit F: Thereafter[F]): EitherT[F, E, R] = {
    val timer = timerMetric.startAsync()
    code.thereafter { _ =>
      timer.stop()
    }
  }

  /** Transform an EitherT into a Future.failed on left
    *
    * Comes handy when having to return io.grpc.StatusRuntimeExceptions
    */
  def toFuture[L <: Throwable, R](x: EitherT[Future, L, R])(implicit
      executionContext: ExecutionContext
  ): Future[R] =
    x.foldF(Future.failed, Future.successful)

  def toFutureUnlessShutdown[L <: Throwable, R](x: EitherT[FutureUnlessShutdown, L, R])(implicit
      executionContext: ExecutionContext
  ): FutureUnlessShutdown[R] =
    x.foldF(FutureUnlessShutdown.failed, FutureUnlessShutdown.pure)

  def unit[A]: EitherT[Future, A, Unit] = EitherT(Future.successful(().asRight[A]))

  def unitUS[A]: EitherT[FutureUnlessShutdown, A, Unit] = EitherT(
    FutureUnlessShutdown.pure(().asRight[A])
  )

  object syntax {
    implicit class FunctorToEitherT[F[_]: Functor, T](f: F[T]) {

      /** Converts any F[T] into EitherT[F, A, T] */
      def toEitherTRight[A]: EitherT[F, A, T] =
        EitherT.right[A](f)
    }
  }
}
