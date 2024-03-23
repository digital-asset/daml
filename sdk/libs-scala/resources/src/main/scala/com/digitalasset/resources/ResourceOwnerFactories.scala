// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.Timer
import java.util.concurrent.{CompletionStage, ExecutorService}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.concurrent.Future
import scala.util.Try

/** Convenient [[AbstractResourceOwner]] factory methods.
  */
trait ResourceOwnerFactories[Context] {
  protected implicit val hasExecutionContext: HasExecutionContext[Context]

  def unit: AbstractResourceOwner[Context, Unit] =
    new FutureResourceOwner(() => Future.unit)

  def successful[T](value: T): AbstractResourceOwner[Context, T] =
    new FutureResourceOwner(() => Future.successful(value))

  def failed(throwable: Throwable): AbstractResourceOwner[Context, Nothing] =
    new FutureResourceOwner(() => Future.failed(throwable))

  def forValue[T](acquire: () => T): AbstractResourceOwner[Context, T] =
    new FutureResourceOwner(() => Future.successful(acquire()))

  def forTry[T](acquire: () => Try[T]): AbstractResourceOwner[Context, T] =
    new FutureResourceOwner(() => Future.fromTry(acquire()))

  def forFuture[T](acquire: () => Future[T]): AbstractResourceOwner[Context, T] =
    new FutureResourceOwner(acquire)

  def forCompletionStage[T](acquire: () => CompletionStage[T]): AbstractResourceOwner[Context, T] =
    new FutureResourceOwner(() => acquire().asScala)

  def forCloseable[T <: AutoCloseable](acquire: () => T): AbstractResourceOwner[Context, T] =
    new CloseableResourceOwner(acquire)

  def forTryCloseable[T <: AutoCloseable](
      acquire: () => Try[T]
  ): AbstractResourceOwner[Context, T] =
    new FutureCloseableResourceOwner(() => Future.fromTry(acquire()))

  def forFutureCloseable[T <: AutoCloseable](
      acquire: () => Future[T]
  ): AbstractResourceOwner[Context, T] =
    new FutureCloseableResourceOwner(acquire)

  def forReleasable[T](acquire: () => T)(
      release: T => Future[Unit]
  ): AbstractResourceOwner[Context, T] =
    new ReleasableResourceOwner(acquire)(release)

  def forExecutorService[T <: ExecutorService](
      acquire: () => T
  ): AbstractResourceOwner[Context, T] =
    new ExecutorServiceResourceOwner(acquire)

  def forTimer(acquire: () => Timer): AbstractResourceOwner[Context, Timer] =
    new TimerResourceOwner(acquire)

}
