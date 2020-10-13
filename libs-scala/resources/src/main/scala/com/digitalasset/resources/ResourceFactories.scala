// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

final class ResourceFactories[Context: HasExecutionContext] {
  import scala.language.higherKinds

  private type R[+T] = Resource[Context, T]

  /**
    * Nests release operation for a [[Resource]]'s future.
    */
  private[resources] def nest[T](future: Future[T])(
      releaseResource: T => Future[Unit],
      releaseSubResources: () => Future[Unit],
  )(implicit context: Context): R[T] = new R[T] {
    final lazy val asFuture: Future[T] = future.transformWith {
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
          .transform( // Finally, complete `releasePromise` to allow other releases to complete
            value => {
              releasePromise.success(())
              value
            },
            exception => {
              releasePromise.success(())
              exception
            },
          )
      else // A release is already in progress or completed; we wait for that instead
        releasePromise.future
  }

  /**
    * Builds a [[Resource]] from a [[Future]] and some release logic.
    */
  def apply[T](future: Future[T])(releaseResource: T => Future[Unit])(
      implicit context: Context): R[T] =
    nest(future)(releaseResource, () => Future.unit)

  /**
    * Wraps a simple [[Future]] in a [[Resource]] that doesn't need to be released.
    */
  def fromFuture[T](future: Future[T])(implicit context: Context): R[T] =
    apply(future)(_ => Future.unit)

  /**
    * Produces a [[Resource]] that has already succeeded with the [[Unit]] value.
    */
  def unit(implicit context: Context): R[Unit] =
    fromFuture(Future.unit)

  /**
    * Produces a [[Resource]] that has already succeeded with a given value.
    */
  def successful[T](value: T)(implicit context: Context): R[T] =
    fromFuture(Future.successful(value))

  /**
    * Produces a [[Resource]] that has already failed with a given exception.
    */
  def failed[T](exception: Throwable)(implicit context: Context): R[T] =
    fromFuture(Future.failed(exception))

  /**
    * Sequences a [[TraversableOnce]] of [[Resource]]s into a [[Resource]] of the [[TraversableOnce]] of their values.
    *
    * @param seq The [[TraversableOnce]] of [[Resource]]s.
    * @param bf The projection from a [[TraversableOnce]] of resources into one of their values.
    * @param context The asynchronous task execution engine.
    * @tparam T The value type.
    * @tparam C The [[TraversableOnce]] actual type.
    * @return A [[Resource]] with a sequence of the values of the sequenced [[Resource]]s as its underlying value.
    */
  def sequence[T, C[X] <: TraversableOnce[X]](seq: C[R[T]])(
      implicit bf: CanBuildFrom[C[R[T]], T, C[T]],
      context: Context,
  ): R[C[T]] =
    seq
      .foldLeft(successful(bf()))((builderResource, elementResource) =>
        for {
          builder <- builderResource // Consider the builder in the accumulator resource
          element <- elementResource // Consider the value in the actual resource element
        } yield builder += element) // Append the element to the builder
      .map(_.result()) // Yield a resource of collection resulting from the builder

  /**
    * Sequences a [[TraversableOnce]] of [[Resource]]s into a [[Resource]] with no underlying value.
    *
    * @param seq The [[TraversableOnce]] of [[Resource]]s.
    * @param context The asynchronous task execution engine.
    * @tparam T The value type.
    * @tparam C The [[TraversableOnce]] actual type.
    * @return A [[Resource]] sequencing the [[Resource]]s and no underlying value.
    */
  def sequenceIgnoringValues[T, C[X] <: TraversableOnce[X]](seq: C[Resource[Context, T]])(
      implicit context: Context
  ): Resource[Context, Unit] =
    seq
      .foldLeft(unit)((builderResource, elementResource) =>
        for {
          _ <- builderResource
          _ <- elementResource
        } yield ())
}
