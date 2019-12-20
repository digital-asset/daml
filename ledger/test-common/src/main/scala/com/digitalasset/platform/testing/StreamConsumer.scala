// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.testing

import com.digitalasset.dec.DirectExecutionContext
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

final class StreamConsumer[A](attach: StreamObserver[A] => Unit) {

  /**
    * THIS WILL NEVER COMPLETE IF FED AN UNBOUND STREAM!!!
    */
  def all(): Future[Vector[A]] = {
    val observer = new FiniteStreamObserver[A]
    attach(observer)
    observer.result
  }

  /**
    * Filters the items coming via the observer and takes the first `n`
    */
  def filterTake(p: A => Boolean)(n: Int): Future[Vector[A]] =
    if (n < 0) {
      Future.failed(new IllegalArgumentException(s"Bad argument $n, non-negative integer required"))
    } else if (n == 0) {
      Future.successful(Vector.empty[A])
    } else {
      val observer = new SizeBoundObserver[A](n, p)
      attach(observer)
      observer.result
    }

  def take(n: Int): Future[Vector[A]] = filterTake(_ => true)(n)

  def find(p: A => Boolean): Future[Option[A]] =
    filterTake(p)(1).map(_.headOption)(DirectExecutionContext)

  def first(): Future[Option[A]] = find(_ => true)

  def within(duration: FiniteDuration)(implicit ec: ExecutionContext): Future[Vector[A]] = {
    val observer = new TimeBoundObserver[A](duration)
    attach(observer)
    observer.result
  }

}
