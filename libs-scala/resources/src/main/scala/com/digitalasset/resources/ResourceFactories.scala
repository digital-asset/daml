// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import com.daml.resources.HasExecutionContext.executionContext

import scala.concurrent.Future
import scala.util.Try

final class ResourceFactories[Context: HasExecutionContext] {

  private type R[+T] = Resource[Context, T]

  /** Builds a [[Resource]] from a [[Future]] and some release logic.
    */
  def apply[T](
      future: Future[T]
  )(releaseResource: T => Future[Unit])(implicit context: Context): R[T] =
    ReleasableResource(future)(releaseResource)

  /** Wraps a simple [[Future]] in a [[Resource]] that doesn't need to be released.
    */
  def fromFuture[T](future: Future[T]): R[T] =
    PureResource(future)

  /** Wraps a simple [[Try]] in a [[Resource]] that doesn't need to be released.
    */
  def fromTry[T](result: Try[T]): R[T] =
    PureResource(Future.fromTry(result))

  /** Produces a [[Resource]] that has already succeeded with the [[Unit]] value.
    */
  def unit: R[Unit] =
    PureResource(Future.unit)

  /** Produces a [[Resource]] that has already succeeded with a given value.
    */
  def successful[T](value: T): R[T] =
    PureResource(Future.successful(value))

  /** Produces a [[Resource]] that has already failed with a given exception.
    */
  def failed[T](exception: Throwable): R[T] =
    PureResource(Future.failed(exception))

  /** Sequences a [[Traversable]] of [[Resource]]s into a [[Resource]] of the [[Traversable]] of their values.
    *
    * @param seq     The [[Traversable]] of [[Resource]]s.
    * @param bf      The projection from a [[Traversable]] of resources into one of their values.
    * @param context The asynchronous task execution engine.
    * @tparam T The value type.
    * @tparam C The [[Traversable]] actual type.
    * @tparam U The return type.
    * @return A [[Resource]] with a sequence of the values of the sequenced [[Resource]]s as its underlying value.
    */
  def sequence[T, C[X] <: Iterable[X], U](seq: C[R[T]])(implicit
      bf: collection.Factory[T, U],
      context: Context,
  ): R[U] = new R[U] {
    private val resource = seq
      .foldLeft(successful(bf.newBuilder))((builderResource, elementResource) =>
        for {
          builder <- builderResource // Consider the builder in the accumulator resource
          element <- elementResource // Consider the value in the actual resource element
        } yield builder += element
      ) // Append the element to the builder
      .map(_.result()) // Yield a resource of collection resulting from the builder

    override def asFuture: Future[U] =
      resource.asFuture

    override def release(): Future[Unit] =
      Future.sequence(seq.map(_.release())).map(_ => ())
  }

  /** Sequences a [[Traversable]] of [[Resource]]s into a [[Resource]] with no underlying value.
    *
    * @param seq     The [[Traversable]] of [[Resource]]s.
    * @param context The asynchronous task execution engine.
    * @tparam T The value type.
    * @tparam C The [[Traversable]] actual type.
    * @return A [[Resource]] sequencing the [[Resource]]s and no underlying value.
    */
  def sequenceIgnoringValues[T, C[X] <: Iterable[X]](seq: C[R[T]])(implicit
      context: Context
  ): R[Unit] =
    sequence(seq)(new UnitCanBuildFrom[T, Nothing], context)

}
