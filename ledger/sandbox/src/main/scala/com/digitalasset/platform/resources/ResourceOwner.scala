// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import java.util.Timer
import java.util.concurrent.ExecutorService

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.{ExecutionContext, Future}

trait ResourceOwner[T] {
  def acquire()(implicit _executionContext: ExecutionContext): Resource[T]
}

object ResourceOwner {
  def pure[T](value: T): ResourceOwner[T] =
    new FutureResourceOwner(() => Future.successful(value))

  def failed[T](exception: Throwable): ResourceOwner[T] =
    new FutureResourceOwner(() => Future.failed(exception))

  def forFuture[T](acquireFuture: () => Future[T]): ResourceOwner[T] =
    new FutureResourceOwner[T](acquireFuture)

  def forCloseable[T <: AutoCloseable](acquireCloseable: () => T): ResourceOwner[T] =
    new CloseableResourceOwner[T](acquireCloseable)

  def forFutureCloseable[T <: AutoCloseable](
      acquireFutureCloseable: () => Future[T]): ResourceOwner[T] =
    new FutureCloseableResourceOwner[T](acquireFutureCloseable)

  def forExecutorService[T <: ExecutorService](acquireExecutorService: () => T): ResourceOwner[T] =
    new ExecutorServiceResourceOwner(acquireExecutorService)

  def forTimer(acquireTimer: () => Timer): ResourceOwner[Timer] =
    new TimerResourceOwner(acquireTimer)

  def forActorSystem(acquireActorSystem: () => ActorSystem): ResourceOwner[ActorSystem] =
    new ActorSystemResourceOwner(acquireActorSystem)

  def forMaterializer(
      acquireMaterializer: () => ActorMaterializer
  ): ResourceOwner[ActorMaterializer] =
    new ActorMaterializerResourceOwner(acquireMaterializer)
}
