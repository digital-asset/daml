// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.resources.ResettableResourceOwner._

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}

class ResettableResourceOwner[A, ResetValue] private (
    owner: Reset => ResetValue => ResourceOwner[A],
    initialValue: ResetValue,
    resetOperation: () => Future[ResetValue],
) extends ResourceOwner[A] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[A] =
    new Resource[A] {
      private val resetFunction: () => Future[Unit] = reset _

      @volatile
      private var resource = owner(resetFunction)(initialValue).acquire()
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
              resource.release().flatMap(_ => resetOperation()).map { resetValue =>
                resource = owner(resetFunction)(resetValue).acquire()
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
      reset => _ => owner(reset),
      initialValue = (),
      resetOperation = () => Future.unit,
    )

  def apply[A](owner: Reset => ResourceOwner[A], resetOperation: () => Future[Unit]) =
    new ResettableResourceOwner[A, Unit](
      reset => _ => owner(reset),
      initialValue = (),
      resetOperation = resetOperation,
    )

  def apply[A, ResetValue](
      owner: Reset => ResetValue => ResourceOwner[A],
      initialValue: ResetValue,
      resetOperation: () => Future[ResetValue],
  ) = new ResettableResourceOwner(owner, initialValue, resetOperation)
}
