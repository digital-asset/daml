// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.atomic.AtomicBoolean

import com.daml.resources.HasExecutionContext.executionContext

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

/** Nests release operation for a [[Resource]]'s future. */
private[resources] final class NestedResource[Context: HasExecutionContext, T] private (
    future: Future[T]
)(
    releaseResource: T => Future[Unit],
    releaseSubResources: () => Future[Unit],
)(implicit context: Context)
    extends Resource[Context, T] {
  lazy val asFuture: Future[T] = future.transformWith {
    case Success(value) => Future.successful(value)
    case Failure(throwable) =>
      release().flatMap(_ => Future.failed(throwable)) // Release everything on failure
  }

  private val released: AtomicBoolean = new AtomicBoolean(false) // Short-circuits to a promise
  private val releasePromise: Promise[Unit] = Promise() // Will be the release return handle

  def release(): Future[Unit] =
    if (released.compareAndSet(false, true))
      // If `release` is called twice, we wait for `releasePromise` to complete instead
      // `released` is set atomically to ensure we don't end up with two concurrent releases
      future
        .transformWith {
          case Success(value) =>
            releaseResource(value).flatMap(_ => releaseSubResources()) // Release all
          case Failure(_) =>
            releaseSubResources() // Only sub-release as the future will take care of itself
        }
        // Finally, complete `releasePromise` to allow other releases to complete
        .andThen { case _ => releasePromise.success(()) }
    else // A release is already in progress or completed; we wait for that instead
      releasePromise.future
}

object NestedResource {
  def apply[Context: HasExecutionContext, T](future: Future[T])(
      releaseResource: T => Future[Unit],
      releaseSubResources: () => Future[Unit],
  )(implicit context: Context): Resource[Context, T] =
    new NestedResource(future)(releaseResource, releaseSubResources)
}
