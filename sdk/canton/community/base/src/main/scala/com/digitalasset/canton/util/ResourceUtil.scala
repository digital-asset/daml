// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.MonadThrow
import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

/** Utility code for doing proper resource management.
  * A lot of it is based on https://medium.com/@dkomanov/scala-try-with-resources-735baad0fd7d
  */
object ResourceUtil {

  /** Does resource management the same way as [[withResource]], but returns an Either instead of throwing exceptions.
    *
    * @param r resource that will be used to derive some value and will be closed automatically in the end
    * @param f function that will be applied to the resource and can possibly throw exceptions
    * @return Either object that contains a Right with the mapped value or a Left with the thrown exception from either
    *         the function or the call to the resource's close method.
    */
  def withResourceEither[T <: AutoCloseable, V](r: => T)(f: T => V): Either[Throwable, V] =
    try {
      Right(withResource(r)(f))
    } catch {
      case NonFatal(e) =>
        Left(e)
    }

  /** The given function is applied to the resource and returned.
    * Resource closing is done automatically after the function is applied.
    * This will rethrow any exception thrown by the given function or the call to the resource's close method,
    * but the resource will be attempted to close no matter what.
    *
    * @param r resource that will be used to derive some value and will be closed automatically in the end
    * @param f function that will be applied to the resource and can possibly throw exceptions
    * @return the result of the given function applied to the resource
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def withResource[T <: AutoCloseable, V](r: => T)(f: T => V): V = {
    val resource: T = r
    var exception: Option[Throwable] = None
    try {
      f(resource)
    } catch {
      case NonFatal(e) =>
        exception = Some(e)
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

  final private[util] class ResourceMonadApplied[M[_]](
      private val dummy: Boolean = true
  ) extends AnyVal {
    def apply[T <: AutoCloseable, V](r: => T)(
        f: T => M[V]
    )(implicit M: MonadThrow[M], TM: Thereafter[M]): M[V] = {
      import Thereafter.syntax.*
      import cats.syntax.flatMap.*
      val resource: T = r
      MonadThrow[M].fromTry(Try(f(resource))).flatten.thereafter(_ => resource.close())
    }
  }

  def withResourceM[M[_]]: ResourceMonadApplied[M] = new ResourceMonadApplied[M]

  def withResourceEitherT[T <: AutoCloseable, E, V, F[_]](r: => T)(f: T => EitherT[Future, E, V])(
      implicit ec: ExecutionContext
  ): EitherT[Future, E, V] = {
    withResourceM(r)(f)
  }

  def withResourceFuture[T <: AutoCloseable, V](r: => T)(f: T => Future[V])(implicit
      ec: ExecutionContext
  ): Future[V] = {
    withResourceM(r)(f)
  }

  def withResourceFutureUS[T <: AutoCloseable, V](r: => T)(f: T => FutureUnlessShutdown[V])(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[V] = {
    withResourceM(r)(f)
  }

  def closeAndAddSuppressed(e: Option[Throwable], resource: AutoCloseable): Unit =
    e.fold(resource.close()) { exception =>
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          exception.addSuppressed(suppressed)
      }
    }
}
