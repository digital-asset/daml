// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.MonadThrow

import scala.util.Try

/** Utility code for doing proper resource management. A lot of it is based on
  * https://medium.com/@dkomanov/scala-try-with-resources-735baad0fd7d
  */
object ResourceUtil {

  /** Does resource management the same way as [[withResource]], but returns an Either instead of
    * throwing exceptions. The exception are fatal exceptions, which are rethrown.
    *
    * @param r
    *   resource that will be used to derive some value and will be closed automatically in the end
    * @param f
    *   function that will be applied to the resource and can possibly throw exceptions
    * @return
    *   Either object that contains a Right with the mapped value or a Left with the thrown
    *   exception from either the function or the call to the resource's close method.
    */
  def withResourceEither[T <: AutoCloseable, V](r: => T)(f: T => V): Either[Throwable, V] =
    Try(withResource(r)(f)).toEither

  /** The given function is applied to the resource and returned. Resource closing is done
    * automatically after the function is applied. This will rethrow any exception thrown by the
    * given function or the call to the resource's close method, but the resource will be attempted
    * to close no matter what.
    *
    * @param r
    *   resource that will be used to derive some value and will be closed automatically in the end
    * @param f
    *   function that will be applied to the resource and can possibly throw exceptions
    * @return
    *   the result of the given function applied to the resource
    */
  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  def withResource[T <: AutoCloseable, V](r: => T)(f: T => V): V =
    withResourceM(r)(resource =>
      // tryCatchAll is analogous to Java's try-with-resource behavior,
      // which also acts on fatal exceptions like OutOfMemoryError.
      // See https://docs.oracle.com/javase/specs/jls/se18/html/jls-14.html#jls-14.20.3
      TryUtil.tryCatchAll(f(resource))
    ).get

  final private[util] class ResourceMonadApplied[M[_]](
      private val dummy: Boolean = true
  ) extends AnyVal {
    def apply[T <: AutoCloseable, V](resource: => T)(
        f: T => M[V]
    )(implicit M: MonadThrow[M], TM: Thereafter[M]): M[V] = {
      import Thereafter.syntax.*
      import cats.syntax.flatMap.*
      lazy val lazyR = resource
      MonadThrow[M].fromTry(Try(f(lazyR))).flatten.thereafter(_ => lazyR.close())
    }
  }

  def withResourceM[M[_]]: ResourceMonadApplied[M] = new ResourceMonadApplied[M]
}
