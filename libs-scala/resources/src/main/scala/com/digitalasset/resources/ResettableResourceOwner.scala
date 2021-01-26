// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.atomic.AtomicReference

import com.daml.resources.ResettableResourceOwner._

import scala.annotation.tailrec
import scala.concurrent.{Future, Promise}

class ResettableResourceOwner[Context: HasExecutionContext, A, ResetValue] private (
    initialValue: ResetValue,
    owner: Reset => ResetValue => AbstractResourceOwner[Context, A],
    resetOperation: A => Future[ResetValue],
) extends AbstractResourceOwner[Context, A] {
  override def acquire()(implicit context: Context): Resource[Context, A] =
    new Resource[Context, A] {
      private val resettableOwner: ResetValue => AbstractResourceOwner[Context, A] = owner(reset _)

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

  def apply[Context: HasExecutionContext, A](owner: Reset => AbstractResourceOwner[Context, A]) =
    new ResettableResourceOwner[Context, A, Unit](
      initialValue = (),
      reset => _ => owner(reset),
      resetOperation = _ => Future.unit,
    )

  def apply[Context: HasExecutionContext, A, ResetValue](
      initialValue: ResetValue,
      owner: Reset => ResetValue => AbstractResourceOwner[Context, A],
      resetOperation: A => Future[ResetValue],
  ) = new ResettableResourceOwner(initialValue, owner, resetOperation)
}
