// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import com.daml.resources.HasExecutionContext.executionContext

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  * A [[Resource]] is a [[Future]] that can be (asynchronously) released and will also release automatically upon failure.
  *
  * @tparam A The type of value being protected as a Resource.
  */
abstract class Resource[Context: HasExecutionContext, +A] {
  self =>

  private type R[+T] = Resource[Context, T]

  private val Resource = new ResourceFactories[Context]

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
  def map[B](f: A => B)(implicit context: Context): R[B] =
    // A mapped Resource is a mapped future plus a nesting of an empty release operation and the actual one
    Resource.nest(asFuture.map(f))(_ => Future.unit, release _)

  /**
    * Just like [[Future]]s, [[Resource]]s can be chained. Both component [[Resource]]s will be released correctly
    * upon failure and explicit release.
    */
  def flatMap[B](f: A => R[B])(implicit context: Context): R[B] = {
    val nextFuture: Future[R[B]] =
      asFuture
        .map(f)
        // Propagate failure through `flatMap`: if `next.asFuture` (i.e. the next resource as a future) fails,
        // `nextFuture` (i.e. the chained resource as a future) should also fail
        .flatMap(next => next.asFuture.map(_ => next))
    val nextRelease = (_: B) =>
      nextFuture.transformWith {
        case Success(b) => b.release() // Release next resource
        case Failure(_) => Future.unit // Already released by future failure
    }
    val future = nextFuture.flatMap(_.asFuture)
    Resource.nest(future)(nextRelease, release _) // Nest next resource release and this resource release
  }

  /**
    * A [[Resource]]'s underlying value can be filtered out and result in a [[Resource]] with a failed [[Future]].
    */
  def withFilter(p: A => Boolean)(implicit context: Context): R[A] = {
    val future = asFuture.flatMap(
      value =>
        if (p(value))
          Future.successful(value)
        else
          Future.failed(new ResourceAcquisitionFilterException()))
    Resource.nest(future)(_ => Future.unit, release _)
  }

  /**
    * A nested resource can be flattened.
    */
  def flatten[B](implicit nestedEvidence: A <:< R[B], context: Context): R[B] =
    flatMap(identity[A])

  /**
    * Just like [[Future]]s, an attempted [[Resource]] computation can transformed.
    */
  def transformWith[B](f: Try[A] => R[B])(implicit context: Context): R[B] =
    Resource
      .nest(asFuture.transformWith(f.andThen(Future.successful)))(
        nested => nested.release(),
        release _,
      )
      .flatten
}
