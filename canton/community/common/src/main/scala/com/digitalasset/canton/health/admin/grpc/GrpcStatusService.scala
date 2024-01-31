// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health.admin.grpc

import better.files.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.health.admin.grpc.GrpcStatusService.DefaultHealthDumpChunkSize
import com.digitalasset.canton.health.admin.v30.{HealthDumpChunk, HealthDumpRequest}
import com.digitalasset.canton.health.admin.{data, v30}
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import io.grpc.stub.StreamObserver

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object GrpcStatusService {
  val DefaultHealthDumpChunkSize: Int =
    1024 * 1024 * 2 // 2MB - This is half of the default max message size of gRPC
}

class GrpcStatusService(
    status: => Future[data.NodeStatus[_]],
    healthDump: () => Future[File],
    processingTimeout: ProcessingTimeout,
)(implicit
    ec: ExecutionContext
) extends v30.StatusServiceGrpc.StatusService {

  override def status(request: Empty): Future[v30.NodeStatus] =
    status.map {
      case data.NodeStatus.Success(status) =>
        v30.NodeStatus(v30.NodeStatus.Response.Success(status.toProtoV30))
      case data.NodeStatus.NotInitialized(active) =>
        v30.NodeStatus(
          v30.NodeStatus.Response.NotInitialized(v30.NodeStatus.NotInitialized(active))
        )
      case data.NodeStatus.Failure(_msg) =>
        // The node's status should never return a Failure here.
        v30.NodeStatus(v30.NodeStatus.Response.Empty)
    }

  override def healthDump(
      request: HealthDumpRequest,
      responseObserver: StreamObserver[HealthDumpChunk],
  ): Unit = {
    // Create a context that will be automatically cancelled after the processing timeout deadline
    val context = io.grpc.Context
      .current()
      .withCancellation()

    context.run { () =>
      val processingResult = healthDump().map { dumpFile =>
        val chunkSize = request.chunkSize.getOrElse(DefaultHealthDumpChunkSize)
        dumpFile.newInputStream
          .buffered(chunkSize)
          .autoClosed { s =>
            Iterator
              .continually(s.readNBytes(chunkSize))
              // Before pushing new chunks to the stream, keep checking that the context has not been cancelled
              // This avoids the server reading the entire dump file for nothing if the client has already cancelled
              .takeWhile(_.nonEmpty && !context.isCancelled)
              .foreach { chunk =>
                responseObserver.onNext(HealthDumpChunk(ByteString.copyFrom(chunk)))
              }
          }
      }

      Try(Await.result(processingResult, processingTimeout.unbounded.duration)) match {
        case Failure(exception) =>
          responseObserver.onError(exception)
          context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
          ()
        case Success(_) =>
          if (!context.isCancelled) responseObserver.onCompleted()
          context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
          ()
      }
    }
  }
}
