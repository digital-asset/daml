// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import scalaz.{Applicative, EitherT, Functor, \/}

import scala.concurrent.Future

object FutureUtil {
  def liftET[E]: LiftET[E] = new LiftET(0)
  final class LiftET[E](private val ignore: Int) extends AnyVal {
    def apply[F[_]: Functor, A](fa: F[A]): EitherT[F, E, A] = EitherT.rightT(fa)
  }

  def eitherT[A, B](fa: Future[A \/ B]): EitherT[Future, A, B] =
    EitherT.eitherT[Future, A, B](fa)

  def either[A, B](d: A \/ B)(implicit ev: Applicative[Future]): EitherT[Future, A, B] =
    EitherT.either[Future, A, B](d)
}
