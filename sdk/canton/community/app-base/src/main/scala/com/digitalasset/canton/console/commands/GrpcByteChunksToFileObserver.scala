// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import better.files.File
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.util.ResourceUtil
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import java.io.FileOutputStream
import scala.concurrent.Promise
import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

private[commands] class GrpcByteChunksToFileObserver[
    T <: GrpcByteChunksToFileObserver.ByteStringChunk
](
    inputFile: File,
    requestComplete: Promise[String],
) extends StreamObserver[T] {
  private val os: FileOutputStream = inputFile.newFileOutputStream(append = false)

  override def onNext(value: T): Unit = {
    Try(os.write(value.chunk.toByteArray)) match {
      case Failure(exception) =>
        ResourceUtil.closeAndAddSuppressed(Some(exception), os)
        throw exception
      case Success(_) => // all good
    }
  }

  override def onError(t: Throwable): Unit = {
    requestComplete.tryFailure(t).discard
    ResourceUtil.closeAndAddSuppressed(None, os)
  }

  override def onCompleted(): Unit = {
    requestComplete.trySuccess(inputFile.pathAsString).discard
    ResourceUtil.closeAndAddSuppressed(None, os)
  }
}

private[commands] object GrpcByteChunksToFileObserver {
  type ByteStringChunk = { val chunk: ByteString }
}
