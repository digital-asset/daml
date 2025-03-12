// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.service

import io.grpc.stub.StreamObserver

import scala.collection.mutable

trait RecordStreamObserverItems[T] {
  this: StreamObserver[T] =>

  val items: mutable.Buffer[StreamItem[T]] = mutable.Buffer[StreamItem[T]]()

  def values: Seq[T] = items.collect { case StreamNext(value) => value }.toSeq

  override def onNext(value: T): Unit = items += StreamNext(value)

  override def onError(t: Throwable): Unit = items += StreamError(t)

  override def onCompleted(): Unit = items += StreamComplete
}

sealed trait StreamItem[+A]

final case class StreamNext[+A](value: A) extends StreamItem[A]
final case class StreamError(t: Throwable) extends StreamItem[Nothing]
object StreamComplete extends StreamItem[Nothing]
