// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.atomic.AtomicReference

import com.daml.resources.ResettableResourceOwner._

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}

class ResettableResourceOwner[A, ResetValue] private (
    initialValue: ResetValue,
    owner: Reset => ResetValue => ResourceOwner[A],
    resetOperation: A => Future[ResetValue],
) extends ResourceOwner[A] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[A] =
    new Resource[A] {
      private val resettableOwner: ResetValue => ResourceOwner[A] = owner(reset _)

      @volatile
      private var resource = resettableOwner(initialValue).acquire()
      private val resetPromise = new AtomicReference[Option[Promise[Unit]]](None)

      override def asFuture: Future[A] =
        resetPromise.get().getOrElse(Promise.successful(())).future.flatMap(_ => resource.asFuture)

      override def release(): Future[Unit] =
        resetPromise.get().getOrElse(Promise.successful(())).future.flatMap(_ => resource.release())

      @tailrec
      private def reset(): Future[Unit] = {
        val currentResetPromise = resetPromise.get()
        currentResetPromise match {
          case None =>
            val newResetPromise = Some(Promise[Unit]())
            if (resetPromise.compareAndSet(None, newResetPromise)) {
              for {
                value <- resource.asFuture
                _ <- resource.release()
                resetValue <- resetOperation(value)
              } yield {
                resource = resettableOwner(resetValue).acquire()
                newResetPromise.get.success(())
                resetPromise.set(None)
              }
            } else {
              reset()
            }
          case Some(currentResetPromise) =>
            currentResetPromise.future
        }
      }
    }
}

object ResettableResourceOwner {
  type Reset = () => Future[Unit]

  def apply[A](owner: Reset => ResourceOwner[A]) =
    new ResettableResourceOwner[A, Unit](
      initialValue = (),
      reset => _ => owner(reset),
      resetOperation = _ => Future.unit,
    )

  def apply[A, ResetValue](
      initialValue: ResetValue,
      owner: Reset => ResetValue => ResourceOwner[A],
      resetOperation: A => Future[ResetValue],
  ) = new ResettableResourceOwner(initialValue, owner, resetOperation)
}
