// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health.admin.grpc

import better.files.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.health.admin.grpc.GrpcStatusService.DefaultHealthDumpChunkSize
import com.digitalasset.canton.health.admin.v0.{HealthDumpChunk, HealthDumpRequest}
import com.digitalasset.canton.health.admin.{data, v0}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NodeLoggingUtil}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import io.grpc.Status
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
    val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends v0.StatusServiceGrpc.StatusService
    with NamedLogging {

  override def status(request: Empty): Future[v0.NodeStatus] =
    status.map {
      case data.NodeStatus.Success(status) =>
        v0.NodeStatus(v0.NodeStatus.Response.Success(status.toProtoV0))
      case data.NodeStatus.NotInitialized(active) =>
        v0.NodeStatus(v0.NodeStatus.Response.NotInitialized(v0.NodeStatus.NotInitialized(active)))
      case data.NodeStatus.Failure(_msg) =>
        // The node's status should never return a Failure here.
        v0.NodeStatus(v0.NodeStatus.Response.Empty)
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

  override def setLogLevel(request: v0.SetLogLevelRequest): Future[v0.SetLogLevelResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    logger.info(s"Changing log level to ${request.level}")
    NodeLoggingUtil.setLevel(level = request.level)
    Future.successful(v0.SetLogLevelResponse())
  }

  override def getLastErrors(request: v0.GetLastErrorsRequest): Future[v0.GetLastErrorsResponse] = {
    NodeLoggingUtil.lastErrors() match {
      case Some(errors) =>
        Future.successful(v0.GetLastErrorsResponse(errors = errors.map { case (traceId, message) =>
          v0.GetLastErrorsResponse.Error(traceId = traceId, message = message)
        }.toSeq))
      case None =>
        Future.failed(
          Status.FAILED_PRECONDITION
            .withDescription("No last errors available")
            .asRuntimeException()
        )
    }
  }

  override def getErrorTrace(
      request: v0.GetLastErrorTraceRequest
  ): Future[v0.GetLastErrorTraceResponse] = {
    NodeLoggingUtil.lastErrorTrace(request.traceId) match {
      case Some(trace) =>
        Future.successful(v0.GetLastErrorTraceResponse(trace))
      case None =>
        Future.failed(
          Status.FAILED_PRECONDITION
            .withDescription("No trace available for the given traceId")
            .asRuntimeException()
        )
    }
  }

}
