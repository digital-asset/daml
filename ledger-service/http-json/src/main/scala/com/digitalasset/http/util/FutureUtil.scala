// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import scalaz.EitherT.rightT
import scalaz.syntax.show._
import scalaz.{-\/, Applicative, EitherT, Functor, Show, \/, \/-}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.Try

object FutureUtil {
  def toFuture[A](o: Option[A]): Future[A] =
    o.fold(Future.failed[A](new IllegalStateException(s"Empty option: $o")))(a =>
      Future.successful(a))

  def toFuture[A](a: Try[A]): Future[A] =
    a.fold(e => Future.failed(e), a => Future.successful(a))

  def toFuture[A: Show, B](a: A \/ B): Future[B] =
    a.fold(e => Future.failed(new IllegalStateException(e.shows)), a => Future.successful(a))

  def liftET[E]: LiftET[E] = new LiftET(0)
  final class LiftET[E](private val ignore: Int) extends AnyVal {
    def apply[F[_]: Functor, A](fa: F[A]): EitherT[F, E, A] = rightT(fa)
  }

  def eitherT[A, B](fa: Future[A \/ B])(implicit ev: Applicative[Future]): EitherT[Future, A, B] =
    EitherT.eitherT[Future, A, B](fa)

  def either[A, B](d: A \/ B)(implicit ev: Applicative[Future]): EitherT[Future, A, B] =
    EitherT.either[Future, A, B](d)

  def stripLeft[A: Show, B](fa: Future[A \/ B])(implicit ec: ExecutionContext): Future[B] =
    fa.flatMap {
      case -\/(e) =>
        Future.failed(new IllegalStateException(e.shows))
      case \/-(a) =>
        Future.successful(a)
    }
}
