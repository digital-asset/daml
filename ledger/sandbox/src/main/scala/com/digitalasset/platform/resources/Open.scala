// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait Open[A] {
  a =>

  val asFuture: Future[A]

  def close(): Future[Unit]

  def map[B](f: A => B)(implicit executionContext: ExecutionContext): Open[B] = new Open[B] {
    override val asFuture: Future[B] =
      a.asFuture.map(f).transformWith(closeOnFailure)

    override def close(): Future[Unit] =
      a.close()
  }

  def flatMap[B](f: A => Open[B])(implicit executionContext: ExecutionContext): Open[B] =
    new Open[B] {
      private val bFuture: Future[Open[B]] =
        a.asFuture
          .map(f)
          .flatMap(b => b.asFuture.map(_ => b)) // if `b.asFuture` fails, `bFuture` should also fail
          .transformWith(closeOnFailure)

      override val asFuture: Future[B] =
        bFuture.flatMap(_.asFuture)

      override def close(): Future[Unit] =
        bFuture.transformWith {
          case Success(b) => b.close().flatMap(_ => a.close())
          case Failure(_) => Future.successful(())
        }
    }

  def withFilter(p: A => Boolean)(implicit executionContext: ExecutionContext): Open[A] =
    new Open[A] {
      override val asFuture: Future[A] =
        a.asFuture.flatMap(
          value =>
            if (p(value))
              Future.successful(value)
            else
              Future.failed(new ResourceAcquisitionFilterException())
        )

      override def close(): Future[Unit] =
        a.close()
    }

  private def closeOnFailure[T](result: Try[T])(
      implicit executionContext: ExecutionContext
  ): Future[T] =
    result match {
      case Success(value) => Future.successful(value)
      case Failure(throwable) => a.close().flatMap(_ => Future.failed(throwable))
    }
}
