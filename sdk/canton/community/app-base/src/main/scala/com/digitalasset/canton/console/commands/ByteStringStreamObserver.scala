// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Future, Promise}
import scala.language.reflectiveCalls

private[commands] class ByteStringStreamObserver[
    T <: ByteStringStreamObserver.ByteStringChunk
] extends StreamObserver[T] {
  private val byteBuffer = new AtomicReference(Vector.empty[Byte])
  private val requestComplete: Promise[ByteString] = Promise[ByteString]()

  def result: Future[ByteString] =
    requestComplete.future

  override def onNext(value: T): Unit =
    byteBuffer.getAndUpdate(_ ++ value.chunk.toByteArray).discard

  override def onError(t: Throwable): Unit = {
    requestComplete.tryFailure(t).discard
  }

  override def onCompleted(): Unit = {
    val finalByteString = ByteString.copyFrom(byteBuffer.get().toArray)
    requestComplete.trySuccess(finalByteString).discard
  }
}

private[commands] object ByteStringStreamObserver {
  type ByteStringChunk = { val chunk: ByteString }
}
