// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown

import scala.concurrent.Future

object EitherUtil {
  implicit class RichEither[L, R](val either: Either[L, R]) extends AnyVal {

    /** @param f
      * @return this, after evaluation of the side effecting function f if this is a left.
      */
    def tapLeft(f: L => Unit): Either[L, R] = either match {
      case Left(value) =>
        f(value)
        either

      case Right(_) => either
    }

    /** @param f
      * @return this, after evaluation of the side effecting function f if this is a right.
      */
    def tapRight(f: R => Unit): Either[L, R] = either match {
      case Right(value) =>
        f(value)
        either

      case Left(_) => either
    }

    def toFuture(f: L => Throwable): Future[R] = either match {
      case Left(value) => Future.failed(f(value))
      case Right(value) => Future.successful(value)
    }

    def toFutureUS(f: L => Throwable): FutureUnlessShutdown[R] = either match {
      case Left(value) => FutureUnlessShutdown.failed(f(value))
      case Right(value) => FutureUnlessShutdown.pure(value)
    }
  }

  implicit class RichEitherIterable[L, R](val eithers: Iterable[Either[L, R]]) extends AnyVal {
    def collectLeft: Iterable[L] = eithers.collect { case Left(value) => value }
    def collectRight: Iterable[R] = eithers.collect { case Right(value) => value }
  }
}
