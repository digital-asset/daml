// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import java.util.Timer
import java.util.concurrent.{CompletionStage, ExecutorService}

import scala.collection.generic.CanBuildFrom
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

@FunctionalInterface
trait ResourceOwner[A] {
  self =>

  def acquire()(implicit executionContext: ExecutionContext): Resource[A]

  def map[B](f: A => B): ResourceOwner[B] = new ResourceOwner[B] {
    override def acquire()(implicit executionContext: ExecutionContext): Resource[B] =
      self.acquire().map(f)
  }

  def flatMap[B](f: A => ResourceOwner[B]): ResourceOwner[B] = new ResourceOwner[B] {
    override def acquire()(implicit executionContext: ExecutionContext): Resource[B] =
      self.acquire().flatMap(value => f(value).acquire())
  }

  def withFilter(p: A => Boolean)(implicit executionContext: ExecutionContext): ResourceOwner[A] =
    new ResourceOwner[A] {
      override def acquire()(implicit executionContext: ExecutionContext): Resource[A] =
        self.acquire().withFilter(p)
    }

  def use[T](behavior: A => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val resource = acquire()
    resource.asFuture
      .flatMap(behavior)
      .transformWith {
        case Success(value) => resource.release().map(_ => value)
        case Failure(exception) => resource.release().flatMap(_ => Future.failed(exception))
      }
  }

  def vary[B >: A]: ResourceOwner[B] = asInstanceOf[ResourceOwner[B]]
}

object ResourceOwner {
  def successful[T](value: T): ResourceOwner[T] =
    forTry(() => Success(value))

  def failed[T](exception: Throwable): ResourceOwner[T] =
    forTry(() => Failure(exception))

  def forTry[T](acquire: () => Try[T]): ResourceOwner[T] =
    new FutureResourceOwner[T](() => Future.fromTry(acquire()))

  def forFuture[T](acquire: () => Future[T]): ResourceOwner[T] =
    new FutureResourceOwner(acquire)

  def forCompletionStage[T](acquire: () => CompletionStage[T]): ResourceOwner[T] =
    new FutureResourceOwner(() => acquire().toScala)

  def forCloseable[T <: AutoCloseable](acquire: () => T): ResourceOwner[T] =
    new CloseableResourceOwner(acquire)

  def forTryCloseable[T <: AutoCloseable](acquire: () => Try[T]): ResourceOwner[T] =
    new FutureCloseableResourceOwner[T](() => Future.fromTry(acquire()))

  def forFutureCloseable[T <: AutoCloseable](acquire: () => Future[T]): ResourceOwner[T] =
    new FutureCloseableResourceOwner(acquire)

  def forExecutorService[T <: ExecutorService](acquire: () => T): ResourceOwner[T] =
    new ExecutorServiceResourceOwner[T](acquire)

  def forTimer(acquire: () => Timer): ResourceOwner[Timer] =
    new TimerResourceOwner(acquire)

  def sequence[T, C[X] <: TraversableOnce[X]](seq: C[ResourceOwner[T]])(
      implicit bf: CanBuildFrom[C[ResourceOwner[T]], T, C[T]],
      executionContext: ExecutionContext,
  ): ResourceOwner[C[T]] =
    seq
      .foldLeft(ResourceOwner.successful(bf()))((builderResource, elementResource) =>
        for {
          builder <- builderResource
          element <- elementResource
        } yield builder += element)
      .map(_.result())

  def sequenceIgnoringValues[T, C[X] <: TraversableOnce[X]](seq: C[ResourceOwner[T]])(
      implicit executionContext: ExecutionContext,
  ): ResourceOwner[Unit] =
    seq
      .foldLeft(ResourceOwner.successful(()))((builderResource, elementResource) =>
        for {
          _ <- builderResource
          _ <- elementResource
        } yield ())
}
