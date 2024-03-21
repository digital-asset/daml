// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import scalaz.syntax.show._
import scalaz.{Applicative, EitherT, Functor, Show, \/}

import scala.concurrent.Future
import scala.util.Try

object FutureUtil {
  def toFuture[A](o: Option[A]): Future[A] =
    o.fold(Future.failed[A](new IllegalStateException(s"Empty option: $o")))(a =>
      Future.successful(a)
    )

  def toFuture[A](a: Try[A]): Future[A] =
    a.fold(e => Future.failed(e), a => Future.successful(a))

  def toFuture[A: Show, B](a: A \/ B): Future[B] =
    a.fold(e => Future.failed(new IllegalStateException(e.shows)), a => Future.successful(a))

  def toFuture[A](a: Throwable \/ A): Future[A] =
    a.fold(e => Future.failed(new IllegalStateException(e)), a => Future.successful(a))

  def liftET[E]: LiftET[E] = new LiftET(0)
  final class LiftET[E](private val ignore: Int) extends AnyVal {
    def apply[F[_]: Functor, A](fa: F[A]): EitherT[F, E, A] = EitherT.rightT(fa)
  }

  def eitherT[A, B](fa: Future[A \/ B]): EitherT[Future, A, B] =
    EitherT.eitherT[Future, A, B](fa)

  def either[A, B](d: A \/ B)(implicit ev: Applicative[Future]): EitherT[Future, A, B] =
    EitherT.either[Future, A, B](d)

  def rightT[A, B](fa: Future[B])(implicit ev: Functor[Future]): EitherT[Future, A, B] =
    EitherT.rightT(fa)

  def leftT[A, B](fa: Future[A])(implicit ev: Functor[Future]): EitherT[Future, A, B] =
    EitherT.leftT(fa)
}
