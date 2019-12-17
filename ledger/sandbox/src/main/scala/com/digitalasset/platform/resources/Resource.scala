// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait Resource[A] {
  self =>

  protected implicit val executionContext: ExecutionContext

  private val released: AtomicBoolean = new AtomicBoolean(false)

  protected val future: Future[A]

  final lazy val asFuture: Future[A] = future.transformWith(releaseOnFailure)

  final def release(): Future[Unit] =
    if (released.compareAndSet(false, true))
      releaseResource()
    else
      Future.successful(())

  def releaseResource(): Future[Unit]

  def map[B](f: A => B)(implicit _executionContext: ExecutionContext): Resource[B] =
    new Resource[B] {
      override protected val executionContext: ExecutionContext = _executionContext

      override protected val future: Future[B] =
        self.asFuture.map(f)

      override def releaseResource(): Future[Unit] =
        self.release()
    }

  def flatMap[B](f: A => Resource[B])(implicit _executionContext: ExecutionContext): Resource[B] =
    new Resource[B] {
      override protected val executionContext: ExecutionContext = _executionContext

      private val nextFuture: Future[Resource[B]] =
        self.asFuture
          .map(f)
          // if `next.asFuture` fails, `nextFuture` should also fail
          .flatMap(next => next.asFuture.map(_ => next).transformWith(releaseOnFailure))

      override protected val future: Future[B] =
        nextFuture.flatMap(_.asFuture)

      override def releaseResource(): Future[Unit] =
        nextFuture.transformWith {
          case Success(b) => b.release().flatMap(_ => self.release())
          case Failure(_) => Future.successful(())
        }
    }

  def withFilter(p: A => Boolean)(implicit _executionContext: ExecutionContext): Resource[A] =
    new Resource[A] {
      override protected val executionContext: ExecutionContext = _executionContext

      override protected val future: Future[A] =
        self.asFuture.flatMap(
          value =>
            if (p(value))
              Future.successful(value)
            else
              Future.failed(new ResourceAcquisitionFilterException())
        )

      override def releaseResource(): Future[Unit] =
        self.release()
    }

  private def releaseOnFailure[T](result: Try[T]): Future[T] =
    result match {
      case Success(value) => Future.successful(value)
      case Failure(throwable) => release().flatMap(_ => Future.failed(throwable))
    }
}

object Resource {

  import scala.language.higherKinds

  def pure[T](value: T)(implicit _executionContext: ExecutionContext): Resource[T] =
    new Resource[T] {
      override protected val executionContext: ExecutionContext = _executionContext

      override protected val future: Future[T] = Future.successful(value)

      override def releaseResource(): Future[Unit] = Future.successful(())
    }

  def failed[T](throwable: Throwable)(implicit _executionContext: ExecutionContext): Resource[T] =
    new Resource[T] {
      override protected val executionContext: ExecutionContext = _executionContext

      override protected val future: Future[T] = Future.failed(throwable)

      override def releaseResource(): Future[Unit] = Future.successful(())
    }

  def sequence[T, C[X] <: TraversableOnce[X]](seq: C[Resource[T]])(
      implicit bf: CanBuildFrom[C[Resource[T]], T, C[T]],
      executionContext: ExecutionContext,
  ): Resource[C[T]] =
    seq
      .foldLeft(Resource.pure(bf()))((builderResource, elementResource) =>
        for {
          builder <- builderResource
          element <- elementResource
        } yield builder += element)
      .map(_.result())

  def sequence_[T, C[X] <: TraversableOnce[X]](seq: C[Resource[T]])(
      implicit executionContext: ExecutionContext,
  ): Resource[Unit] =
    seq
      .foldLeft(Resource.pure(()))((builderResource, elementResource) =>
        for {
          _ <- builderResource
          _ <- elementResource
        } yield ())
}
