// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.Timer
import java.util.concurrent.{CompletionStage, ExecutorService}

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.util.Try

/**
  * Convenient [[ResourceOwner]] factory methods.
  */
trait ResourceOwnerFactories {
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
