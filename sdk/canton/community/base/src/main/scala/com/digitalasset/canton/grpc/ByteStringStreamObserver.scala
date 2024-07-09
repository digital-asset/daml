// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.grpc

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

class ByteStringStreamObserver[T](converter: T => ByteString)
    extends ByteStringStreamObserverWithContext[T, Unit](converter, _ => ()) {
  def resultBytes(implicit ec: ExecutionContext): Future[ByteString] = result.map(_._1)
}

// This observer allows extracting a bytestring, as well as other fields that are part of the request.
// It expects these fields to remain unchanged during the processing of the stream.
class ByteStringStreamObserverWithContext[T, Context](
    converter: T => ByteString,
    extractContext: T => Context,
) extends StreamObserver[T] {
  private val byteBuffer = new AtomicReference(ByteString.EMPTY)
  private val requestComplete: Promise[(ByteString, Context)] = Promise[(ByteString, Context)]()

  val context = new AtomicReference[Option[Context]](None)

  def result: Future[(ByteString, Context)] =
    requestComplete.future

  private def setOrCheck(current: Context): Try[Unit] =
    if (!context.compareAndSet(None, Some(current))) {
      val previous = context.get()
      if (previous.contains(current)) {
        Success(())
      } else {
        Failure(new IllegalStateException(s"Context cannot be changed from: $previous to $current"))
      }
    } else {
      Success(())
    }

  override def onNext(value: T): Unit = {
    val processRequest =
      for {
        _ <- setOrCheck(extractContext(value))
        _ <- Try(byteBuffer.getAndUpdate(b1 => b1.concat(converter(value))).discard)
      } yield ()
    processRequest match {
      case Failure(exception) => requestComplete.failure(exception)
      case Success(_) => () // Nothing to do, just move on to the next request
    }
  }

  override def onError(t: Throwable): Unit = {
    requestComplete.tryFailure(t).discard
  }

  override def onCompleted(): Unit = {
    val finalByteString = byteBuffer.get()
    val finalResult =
      context
        .get()
        .map(Success(_))
        .getOrElse(Failure(new IllegalStateException("Context not set")))
        .map((finalByteString, _))
    requestComplete.tryComplete(finalResult).discard
  }
}
