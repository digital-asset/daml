// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import better.files.DisposeableExtensions
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import java.io.{BufferedInputStream, ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object GrpcUtils {
  private final val defaultChunkSize: Int =
    1024 * 1024 * 2 // 2MB - This is half of the default max message size of gRPC

  def streamResponse[T](
      responseF: OutputStream => Future[Unit],
      responseObserver: StreamObserver[T],
      fromByteString: FromByteString[T],
      processingTimeout: Duration,
      chunkSizeO: Option[Int] = None,
  )(implicit ec: ExecutionContext): Unit = {
    val context = io.grpc.Context
      .current()
      .withCancellation()

    val outputStream = new ByteArrayOutputStream()
    context.run { () =>
      val processingResult = responseF(outputStream).map { _ =>
        val chunkSize = chunkSizeO.getOrElse(defaultChunkSize)
        val inputStream = new ByteArrayInputStream(outputStream.toByteArray)
        new BufferedInputStream(inputStream)
          .autoClosed { s =>
            Iterator
              .continually(s.readNBytes(chunkSize))
              // Before pushing new chunks to the stream, keep checking that the context has not been cancelled
              // This avoids the server reading the entire dump file for nothing if the client has already cancelled
              .takeWhile(_.nonEmpty && !context.isCancelled)
              .foreach { byteArray =>
                val chunk: ByteString = ByteString.copyFrom(byteArray)
                responseObserver.onNext(fromByteString.toT(chunk))
              }
          }
      }

      Try(Await.result(processingResult, processingTimeout)) match {
        case Failure(exception) =>
          responseObserver.onError(exception)
          context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
          ()
        case Success(_) =>
          if (!context.isCancelled) responseObserver.onCompleted()
          else {
            context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
            ()
          }
      }
    }
  }
}

// Define a type class for converting ByteString to the generic type T
trait FromByteString[T] {
  def toT(chunk: ByteString): T
}
