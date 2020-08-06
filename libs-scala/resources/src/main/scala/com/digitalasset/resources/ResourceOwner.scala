// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * A [[ResourceOwner]] of type [[A]] is can acquire a [[Resource]] of the same type and its operations are applied to
  * the [[Resource]] after it has been acquired.
  *
  * @tparam A The [[Resource]] value type.
  */
@FunctionalInterface
trait ResourceOwner[+A] {
  self =>

  /**
    * Acquires the [[Resource]].
    *
    * @param executionContext The asynchronous task execution engine.
    * @return The acquired [[Resource]].
    */
  def acquire()(implicit executionContext: ExecutionContext): Resource[A]

  /** @see [[Resource.map()]] */
  def map[B](f: A => B): ResourceOwner[B] = new ResourceOwner[B] {
    override def acquire()(implicit executionContext: ExecutionContext): Resource[B] =
      self.acquire().map(f)
  }

  /** @see [[Resource.flatMap()]] */
  def flatMap[B](f: A => ResourceOwner[B]): ResourceOwner[B] = new ResourceOwner[B] {
    override def acquire()(implicit executionContext: ExecutionContext): Resource[B] =
      self.acquire().flatMap(value => f(value).acquire())
  }

  /** @see [[Resource.withFilter()]] */
  def withFilter(p: A => Boolean): ResourceOwner[A] =
    new ResourceOwner[A] {
      override def acquire()(implicit executionContext: ExecutionContext): Resource[A] =
        self.acquire().withFilter(p)
    }

  /**
    * Acquire the [[Resource]]'s value, use it asynchronously, and release it afterwards.
    *
    * @param behavior The aynchronous computation on the value.
    * @param executionContext The asynchronous task execution engine.
    * @tparam T The asynchronous computation's value type.
    * @return The asynchronous computation's [[Future]].
    */
  def use[T](behavior: A => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val resource = acquire()
    resource.asFuture
      .flatMap(behavior)
      .transformWith { // Release the resource whether the computation succeeds or not
        case Success(value) => resource.release().map(_ => value)
        case Failure(exception) => resource.release().flatMap(_ => Future.failed(exception))
      }
  }
}
