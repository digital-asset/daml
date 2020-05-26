// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * A [[Resource]] is a [[Future]] that can be (asynchronously) released and will also release automatically upon failure.
  *
  * @tparam A The type of value being protected as a Resource.
  */
trait Resource[+A] {
  self =>

  /**
    * Every [[Resource]] has an underlying [[Future]] representation.
    */
  def asFuture: Future[A]

  /**
    * Every [[Resource]] can be (asynchronously) released. Releasing a resource will also release
    * all earlier resources constructed via [[flatMap()]] or a `for` comprehension.
    */
  def release(): Future[Unit]

  /**
    * The underlying [[Future]] value in a [[Resource]] can be transformed.
    */
  def map[B](f: A => B)(implicit executionContext: ExecutionContext): Resource[B] =
    // A mapped Resource is a mapped future plus a nesting of an empty release operation and the actual one
    Resource.nest(asFuture.map(f))(_ => Future.successful(()), release _)

  /**
    * Just like [[Future]]s, [[Resource]]s can be chained. Both component [[Resource]]s will be released correctly
    * upon failure and explicit release.
    */
  def flatMap[B](f: A => Resource[B])(implicit executionContext: ExecutionContext): Resource[B] = {
    val nextFuture: Future[Resource[B]] =
      asFuture
        .map(f)
        // Propagate failure through `flatMap`: if `next.asFuture` (i.e. the next resource as a future) fails,
        // `nextFuture` (i.e. the chained resource as a future) should also fail
        .flatMap(next => next.asFuture.map(_ => next))
    val nextRelease = (_: B) =>
      nextFuture.transformWith {
        case Success(b) => b.release() // Release next resource
        case Failure(_) => Future.successful(()) // Already released by future failure
    }
    val future = nextFuture.flatMap(_.asFuture)
    Resource.nest(future)(nextRelease, release _) // Nest next resource release and this resource release
  }

  /**
    * A [[Resource]]'s underlying value can be filtered out and result in a [[Resource]] with a failed [[Future]].
    */
  def withFilter(p: A => Boolean)(implicit executionContext: ExecutionContext): Resource[A] = {
    val future = asFuture.flatMap(
      value =>
        if (p(value))
          Future.successful(value)
        else
          Future.failed(new ResourceAcquisitionFilterException()))
    Resource.nest(future)(_ => Future.successful(()), release _)
  }

  /**
    * A nested resource can be flattened.
    */
  def flatten[B](
      implicit nestedEvidence: A <:< Resource[B],
      executionContext: ExecutionContext,
  ): Resource[B] =
    flatMap(identity[A])

  /**
    * Just like [[Future]]s, an attempted [[Resource]] computation can transformed.
    */
  def transformWith[B](f: Try[A] => Resource[B])(
      implicit executionContext: ExecutionContext,
  ): Resource[B] =
    Resource
      .nest(asFuture.transformWith(f.andThen(Future.successful)))(
        (nested: Resource[B]) => nested.release(),
        release _,
      )
      .flatten
}

object Resource {
  import scala.language.higherKinds

  /**
    * Nests release operation for a [[Resource]]'s future.
    */
  private def nest[T](future: Future[T])(
      releaseResource: T => Future[Unit],
      releaseSubResources: () => Future[Unit],
  )(implicit executionContext: ExecutionContext): Resource[T] =
    new Resource[T] {

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
      implicit executionContext: ExecutionContext
  ): Resource[T] =
    nest(future)(releaseResource, () => Future.successful(()))

  /**
    * Wraps a simple [[Future]] in a [[Resource]] that doesn't need to be released.
    */
  def fromFuture[T](future: Future[T])(implicit executionContext: ExecutionContext): Resource[T] =
    apply(future)(_ => Future.successful(()))

  /**
    * Produces a [[Resource]] that has already succeeded with the [[Unit]] value.
    */
  def unit(implicit executionContext: ExecutionContext): Resource[Unit] =
    Resource.fromFuture(Future.unit)

  /**
    * Produces a [[Resource]] that has already succeeded with a given value.
    */
  def successful[T](value: T)(implicit executionContext: ExecutionContext): Resource[T] =
    Resource.fromFuture(Future.successful(value))

  /**
    * Produces a [[Resource]] that has already failed with a given exception.
    */
  def failed[T](exception: Throwable)(implicit executionContext: ExecutionContext): Resource[T] =
    Resource.fromFuture(Future.failed(exception))

  /**
    * Sequences a [[TraversableOnce]] of [[Resource]]s into a [[Resource]] of the [[TraversableOnce]] of their values.
    *
    * @param seq The [[TraversableOnce]] of [[Resource]]s.
    * @param bf The projection from a [[TraversableOnce]] of resources into one of their values.
    * @param executionContext The asynchronous task execution engine.
    * @tparam T The value type.
    * @tparam C The [[TraversableOnce]] actual type.
    * @return A [[Resource]] with a sequence of the values of the sequenced [[Resource]]s as its underlying value.
    */
  def sequence[T, C[X] <: TraversableOnce[X]](seq: C[Resource[T]])(
      implicit bf: CanBuildFrom[C[Resource[T]], T, C[T]],
      executionContext: ExecutionContext,
  ): Resource[C[T]] =
    seq
      .foldLeft(Resource.successful(bf()))((builderResource, elementResource) =>
        for {
          builder <- builderResource // Consider the builder in the accumulator resource
          element <- elementResource // Consider the value in the actual resource element
        } yield builder += element) // Append the element to the builder
      .map(_.result()) // Yield a resource of collection resulting from the builder

  /**
    * Sequences a [[TraversableOnce]] of [[Resource]]s into a [[Resource]] with no underlying value.
    *
    * @param seq The [[TraversableOnce]] of [[Resource]]s.
    * @param executionContext The asynchronous task execution engine.
    * @tparam T The value type.
    * @tparam C The [[TraversableOnce]] actual type.
    * @return A [[Resource]] sequencing the [[Resource]]s and no underlying value.
    */
  def sequenceIgnoringValues[T, C[X] <: TraversableOnce[X]](seq: C[Resource[T]])(
      implicit executionContext: ExecutionContext,
  ): Resource[Unit] =
    seq
      .foldLeft(Resource.unit)((builderResource, elementResource) =>
        for {
          _ <- builderResource
          _ <- elementResource
        } yield ())
}
