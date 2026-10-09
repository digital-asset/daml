// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.grpc

import better.files.File
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.TryUtil.*
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class FileStreamObserver[T](
    inputFile: File,
    converter: T => ByteString,
) extends StreamObserver[T] {
  private val os = inputFile.newFileOutputStream(append = false)
  private val requestComplete: Promise[Unit] = Promise[Unit]()

  def result: Future[Unit] = requestComplete.future
  override def onNext(value: T): Unit =
    Try(os.write(converter(value).toByteArray)) match {
      case Failure(exception) =>
        Try(os.close()).forFailed { suppressed =>
          // Avoid an IllegalArgumentException if it's the same exception,
          if (!(suppressed eq exception)) exception.addSuppressed(suppressed)
        }
        throw exception
      case Success(_) => // all good
    }

  override def onError(t: Throwable): Unit = {
    requestComplete.tryFailure(t).discard
    os.close()
  }

  override def onCompleted(): Unit = {
    requestComplete.trySuccess(()).discard
    os.close()
  }
}
