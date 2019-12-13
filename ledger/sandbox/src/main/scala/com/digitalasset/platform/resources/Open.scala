// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait Open[A] {
  self =>

  protected implicit val executionContext: ExecutionContext

  protected val future: Future[A]

  lazy val asFuture: Future[A] = future.transformWith(closeOnFailure)

  def close(): Future[Unit]

  def map[B](f: A => B)(implicit _executionContext: ExecutionContext): Open[B] =
    new Open[B] {
      override protected val executionContext: ExecutionContext = _executionContext

      override protected val future: Future[B] =
        self.asFuture.map(f)

      override def close(): Future[Unit] =
        self.close()
    }

  def flatMap[B](f: A => Open[B])(implicit _executionContext: ExecutionContext): Open[B] =
    new Open[B] {
      override protected val executionContext: ExecutionContext = _executionContext

      private val nextFuture: Future[Open[B]] =
        self.asFuture
          .map(f)
          // if `next.asFuture` fails, `nextFuture` should also fail
          .flatMap(next => next.asFuture.map(_ => next).transformWith(closeOnFailure))

      override protected val future: Future[B] =
        nextFuture.flatMap(_.asFuture)

      override def close(): Future[Unit] =
        nextFuture.transformWith {
          case Success(b) => b.close().flatMap(_ => self.close())
          case Failure(_) => Future.successful(())
        }
    }

  def withFilter(p: A => Boolean)(implicit _executionContext: ExecutionContext): Open[A] =
    new Open[A] {
      override protected val executionContext: ExecutionContext = _executionContext

      override protected val future: Future[A] =
        self.asFuture.flatMap(
          value =>
            if (p(value))
              Future.successful(value)
            else
              Future.failed(new ResourceAcquisitionFilterException())
        )

      override def close(): Future[Unit] =
        self.close()
    }

  private def closeOnFailure[T](result: Try[T]): Future[T] =
    result match {
      case Success(value) => Future.successful(value)
      case Failure(throwable) => close().flatMap(_ => Future.failed(throwable))
    }
}
