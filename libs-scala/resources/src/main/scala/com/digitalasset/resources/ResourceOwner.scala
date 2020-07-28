// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.Timer
import java.util.concurrent.{CompletionStage, ExecutorService}

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * A [[ResourceOwner]] of type [[A]] is can acquire a [[Resource]] of the same type and its operations are applied to
  * the [[Resource]] after it has been acquired.
  *
  * @tparam A The [[Resource]] value type.
  */
@FunctionalInterface
trait ResourceOwner[+A] {
  self =>

  /**
    * Acquires the [[Resource]].
    *
    * @param executionContext The asynchronous task execution engine.
    * @return The acquired [[Resource]].
    */
  def acquire()(implicit executionContext: ExecutionContext): Resource[A]

  /** @see [[Resource.map()]] */
  def map[B](f: A => B): ResourceOwner[B] = new ResourceOwner[B] {
    override def acquire()(implicit executionContext: ExecutionContext): Resource[B] =
      self.acquire().map(f)
  }

  /** @see [[Resource.flatMap()]] */
  def flatMap[B](f: A => ResourceOwner[B]): ResourceOwner[B] = new ResourceOwner[B] {
    override def acquire()(implicit executionContext: ExecutionContext): Resource[B] =
      self.acquire().flatMap(value => f(value).acquire())
  }

  /** @see [[Resource.withFilter()]] */
  def withFilter(p: A => Boolean)(implicit executionContext: ExecutionContext): ResourceOwner[A] =
    new ResourceOwner[A] {
      override def acquire()(implicit executionContext: ExecutionContext): Resource[A] =
        self.acquire().withFilter(p)
    }

  /**
    * Acquire the [[Resource]]'s value, use it asynchronously, and release it afterwards.
    *
    * @param behavior The aynchronous computation on the value.
    * @param executionContext The asynchronous task execution engine.
    * @tparam T The asynchronous computation's value type.
    * @return The asynchronous computation's [[Future]].
    */
  def use[T](behavior: A => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val resource = acquire()
    resource.asFuture
      .flatMap(behavior)
      .transformWith { // Release the resource whether the computation succeeds or not
        case Success(value) => resource.release().map(_ => value)
        case Failure(exception) => resource.release().flatMap(_ => Future.failed(exception))
      }
  }
}

/**
  * Convenient [[ResourceOwner]] factory and sequencing methods.
  */
object ResourceOwner {
  def unit: ResourceOwner[Unit] =
    new FutureResourceOwner(() => Future.unit)

  def successful[T](value: T): ResourceOwner[T] =
    new FutureResourceOwner(() => Future.successful(value))

  def failed(throwable: Throwable): ResourceOwner[Nothing] =
    new FutureResourceOwner(() => Future.failed(throwable))

  def forValue[T](acquire: () => T): ResourceOwner[T] =
    new FutureResourceOwner(() => Future.successful(acquire()))

  def forTry[T](acquire: () => Try[T]): ResourceOwner[T] =
    new FutureResourceOwner(() => Future.fromTry(acquire()))

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
    new ExecutorServiceResourceOwner(acquire)

  def forTimer(acquire: () => Timer): ResourceOwner[Timer] =
    new TimerResourceOwner(acquire)
}
