// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

trait Resource[+A] {
  self =>

  val asFuture: Future[A]

  def release(): Future[Unit]

  def map[B](f: A => B)(implicit executionContext: ExecutionContext): Resource[B] =
    Resource.nest(asFuture.map(f))(_ => Future.successful(()), release _)

  def flatMap[B](f: A => Resource[B])(implicit executionContext: ExecutionContext): Resource[B] = {
    val nextFuture: Future[Resource[B]] =
      asFuture
        .map(f)
        // if `next.asFuture` fails, `nextFuture` should also fail
        .flatMap(next => next.asFuture.map(_ => next))
    val nextRelease = (_: B) =>
      nextFuture.transformWith {
        case Success(b) => b.release()
        case Failure(_) => Future.successful(())
    }
    Resource.nest(nextFuture.flatMap(_.asFuture))(nextRelease, release _)
  }

  def withFilter(p: A => Boolean)(implicit executionContext: ExecutionContext): Resource[A] = {
    val future = asFuture.flatMap(
      value =>
        if (p(value))
          Future.successful(value)
        else
          Future.failed(new ResourceAcquisitionFilterException()))
    Resource.nest(future)(_ => Future.successful(()), release _)
  }

  def flatten[B](
      implicit nestedEvidence: A <:< Resource[B],
      executionContext: ExecutionContext,
  ): Resource[B] =
    flatMap(identity[A])

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

  private def nest[T](future: Future[T])(
      releaseResource: T => Future[Unit],
      releaseSubResources: () => Future[Unit],
  )(implicit executionContext: ExecutionContext): Resource[T] =
    new Resource[T] {
      private val released: AtomicBoolean = new AtomicBoolean(false)
      private val releasePromise: Promise[Unit] = Promise()

      final lazy val asFuture: Future[T] = future.transformWith {
        case Success(value) => Future.successful(value)
        case Failure(throwable) => release().flatMap(_ => Future.failed(throwable))
      }

      def release(): Future[Unit] =
        if (released.compareAndSet(false, true))
          future
            .transformWith {
              case Success(value) => releaseResource(value).flatMap(_ => releaseSubResources())
              case Failure(_) => releaseSubResources()
            }
            .transform(
              value => {
                releasePromise.success(())
                value
              },
              exception => {
                releasePromise.success(())
                exception
              },
            )
        else
          releasePromise.future
    }

  def apply[T](future: Future[T])(releaseResource: T => Future[Unit])(
      implicit executionContext: ExecutionContext
  ): Resource[T] =
    nest(future)(releaseResource, () => Future.successful(()))

  /**
    * Useful to wrap a simple [[Future]] that doesn't need releasing in a [[Resource]]
    */
  def fromFuture[T](future: Future[T])(implicit executionContext: ExecutionContext): Resource[T] =
    apply(future)(_ => Future.successful(()))

  def successful[T](value: T)(implicit executionContext: ExecutionContext): Resource[T] =
    Resource.fromFuture(Future.successful(value))

  def failed[T](exception: Throwable)(implicit executionContext: ExecutionContext): Resource[T] =
    Resource.fromFuture(Future.failed(exception))

  def sequence[T, C[X] <: TraversableOnce[X]](seq: C[Resource[T]])(
      implicit bf: CanBuildFrom[C[Resource[T]], T, C[T]],
      executionContext: ExecutionContext,
  ): Resource[C[T]] =
    seq
      .foldLeft(Resource.successful(bf()))((builderResource, elementResource) =>
        for {
          builder <- builderResource
          element <- elementResource
        } yield builder += element)
      .map(_.result())

  def sequenceIgnoringValues[T, C[X] <: TraversableOnce[X]](seq: C[Resource[T]])(
      implicit executionContext: ExecutionContext,
  ): Resource[Unit] =
    seq
      .foldLeft(Resource.successful(()))((builderResource, elementResource) =>
        for {
          _ <- builderResource
          _ <- elementResource
        } yield ())
}
