// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.arrow.FunctionK
import cats.{Applicative, Monad, Parallel, ~>}

import scala.concurrent.{ExecutionContext, Future}

object FutureInstances {

  def parallelApplicativeFuture(implicit ec: ExecutionContext): Applicative[Future] =
    new Applicative[Future] {
      override def pure[A](x: A): Future[A] = Future.successful(x)

      override def ap[A, B](ff: Future[A => B])(fa: Future[A]): Future[B] =
        ff.zipWith(fa)(_.apply(_))

      override def product[A, B](fa: Future[A], fb: Future[B]): Future[(A, B)] = fa.zip(fb)
    }

  implicit def parallelFuture(implicit ec: ExecutionContext): Parallel[Future] =
    new Parallel[Future] {
      override type F[X] = Future[X]

      def parallel: Future ~> Future = FunctionK.id

      def sequential: Future ~> Future = FunctionK.id

      // The standard applicative instance on Future that runs everything in parallel
      def applicative: Applicative[Future] = parallelApplicativeFuture

      // The Cats monad instance for Future runs applicative operations sequentially since 2.7.0
      // but there are no guarantees w.r.t. the evaluation behaviour.
      def monad: Monad[Future] = Monad[Future]
    }

}
