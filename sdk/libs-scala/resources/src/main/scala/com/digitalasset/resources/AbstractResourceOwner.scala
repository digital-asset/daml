// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** A ResourceOwner of type `A` is can acquire a [[Resource]] of the same type and its operations
  * are applied to the [[Resource]] after it has been acquired.
  *
  * @tparam A The [[Resource]] value type.
  */
abstract class AbstractResourceOwner[Context: HasExecutionContext, +A] {
  self =>

  private type R[+T] = AbstractResourceOwner[Context, T]

  protected implicit def executionContext(implicit context: Context): ExecutionContext =
    HasExecutionContext.executionContext

  /** Acquires the [[Resource]].
    *
    * @param context The acquisition context, including the asynchronous task execution engine.
    * @return The acquired [[Resource]].
    */
  def acquire()(implicit context: Context): Resource[Context, A]

  /** @see [[Resource.map]] */
  def map[B](f: A => B): R[B] = new R[B] {
    override def acquire()(implicit context: Context): Resource[Context, B] =
      self.acquire().map(f)
  }

  /** @see [[Resource.flatMap]] */
  def flatMap[B](f: A => R[B]): R[B] = new R[B] {
    override def acquire()(implicit context: Context): Resource[Context, B] =
      self.acquire().flatMap(value => f(value).acquire())
  }

  /** @see [[Resource.withFilter]] */
  def withFilter(p: A => Boolean): R[A] = new R[A] {
    override def acquire()(implicit context: Context): Resource[Context, A] =
      self.acquire().withFilter(p)
  }

  /** @see [[Resource.transform]] */
  def transform[B](f: Try[A] => Try[B]): R[B] = new R[B] {
    override def acquire()(implicit context: Context): Resource[Context, B] =
      self.acquire().transform(f)
  }

  /** @see [[Resource.transformWith]] */
  def transformWith[B](f: Try[A] => R[B]): R[B] = new R[B] {
    override def acquire()(implicit context: Context): Resource[Context, B] =
      self.acquire().transformWith(result => f(result).acquire())
  }

  /** Acquire the [[Resource]]'s value, use it asynchronously, and release it afterwards.
    *
    * @param behavior The asynchronous computation on the value.
    * @param context  The acquisition context, including the asynchronous task execution engine.
    * @tparam T The asynchronous computation's value type.
    * @return The asynchronous computation's [[Future]].
    */
  def use[T](behavior: A => Future[T])(implicit context: Context): Future[T] = {
    val resource = acquire()
    resource.asFuture
      .flatMap(behavior)
      .transformWith { // Release the resource whether the computation succeeds or not
        case Success(value) => resource.release().map(_ => value)
        case Failure(exception) => resource.release().flatMap(_ => Future.failed(exception))
      }
  }
}
