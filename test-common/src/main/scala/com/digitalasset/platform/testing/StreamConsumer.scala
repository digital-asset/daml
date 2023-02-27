// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import io.grpc.stub.StreamObserver

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

final class StreamConsumer[A](attach: StreamObserver[A] => Unit) {

  /** THIS WILL NEVER COMPLETE IF FED AN UNBOUND STREAM!!!
    */
  def all(): Future[Vector[A]] = {
    val observer = new FiniteStreamObserver[A]
    attach(observer)
    observer.result
  }

  /** Filters the items coming via the observer and takes the first N.
    */
  def filterTake(predicate: A => Boolean)(sizeCap: Int): Future[Vector[A]] = {
    val finiteObserver = new FiniteStreamObserver[A]
    val sizeBoundObserver = new SizeBoundObserver(sizeCap)(finiteObserver)
    val filteringObserver = new ObserverFilter(predicate)(sizeBoundObserver)
    attach(filteringObserver)
    finiteObserver.result
  }

  def take(sizeCap: Int): Future[Vector[A]] = {
    val observer = new FiniteStreamObserver[A]
    attach(new SizeBoundObserver(sizeCap)(observer))
    observer.result
  }

  def find(predicate: A => Boolean)(implicit ec: ExecutionContext): Future[A] =
    filterTake(predicate)(sizeCap = 1).map(_.head)

  def first()(implicit ec: ExecutionContext): Future[Option[A]] =
    take(1).map(_.headOption)

  def within(duration: FiniteDuration)(implicit ec: ExecutionContext): Future[Vector[A]] = {
    val observer = new FiniteStreamObserver[A]
    attach(new TimeBoundObserver(duration)(observer))
    observer.result
  }

  def firstWithin(duration: FiniteDuration)(implicit ec: ExecutionContext): Future[Vector[A]] = {
    val observer = new FiniteStreamObserver[A]
    attach(new TimeBoundObserver(duration)(new SizeBoundObserver(sizeCap = 1)(observer)))
    observer.result
  }

}
