// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health.admin.grpc

import better.files.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.health.admin.v30.{HealthDumpRequest, HealthDumpResponse}
import com.digitalasset.canton.health.admin.{data, v30}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NodeLoggingUtil}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.GrpcStreamingUtils
import io.grpc.Status
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

object GrpcStatusService {
  val DefaultHealthDumpChunkSize: Int =
    1024 * 1024 * 2 // 2MB - This is half of the default max message size of gRPC
}

class GrpcStatusService(
    status: => Future[data.NodeStatus[_]],
    healthDump: File => Future[Unit],
    processingTimeout: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends v30.StatusServiceGrpc.StatusService
    with NamedLogging {

  override def status(request: v30.StatusRequest): Future[v30.StatusResponse] =
    status.map {
      case data.NodeStatus.Success(status) =>
        v30.StatusResponse(v30.StatusResponse.Response.Success(status.toProtoV30))
      case data.NodeStatus.NotInitialized(active, waitingFor) =>
        v30.StatusResponse(
          v30.StatusResponse.Response.NotInitialized(
            v30.StatusResponse.NotInitialized(
              active,
              waitingFor
                .map(_.toProtoV30)
                .getOrElse(
                  v30.StatusResponse.NotInitialized.WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_UNSPECIFIED
                ),
            )
          )
        )
      case data.NodeStatus.Failure(_msg) =>
        // The node's status should never return a Failure here.
        v30.StatusResponse(v30.StatusResponse.Response.Empty)
    }

  override def healthDump(
      request: HealthDumpRequest,
      responseObserver: StreamObserver[HealthDumpResponse],
  ): Unit = {
    GrpcStreamingUtils.streamToClientFromFile(
      (file: File) => healthDump(file),
      responseObserver,
      byteString => HealthDumpResponse(byteString),
      processingTimeout.unbounded.duration,
    )
  }

  override def setLogLevel(request: v30.SetLogLevelRequest): Future[v30.SetLogLevelResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    logger.info(s"Changing log level to ${request.level}")
    NodeLoggingUtil.setLevel(level = request.level)
    Future.successful(v30.SetLogLevelResponse())
  }

  override def getLastErrors(
      request: v30.GetLastErrorsRequest
  ): Future[v30.GetLastErrorsResponse] = {
    NodeLoggingUtil.lastErrors() match {
      case Some(errors) =>
        Future.successful(v30.GetLastErrorsResponse(errors = errors.map { case (traceId, message) =>
          v30.GetLastErrorsResponse.Error(traceId = traceId, message = message)
        }.toSeq))
      case None =>
        Future.failed(
          Status.FAILED_PRECONDITION
            .withDescription("No last errors available")
            .asRuntimeException()
        )
    }
  }

  override def getLastErrorTrace(
      request: v30.GetLastErrorTraceRequest
  ): Future[v30.GetLastErrorTraceResponse] = {
    NodeLoggingUtil.lastErrorTrace(request.traceId) match {
      case Some(trace) =>
        Future.successful(v30.GetLastErrorTraceResponse(trace))
      case None =>
        Future.failed(
          Status.FAILED_PRECONDITION
            .withDescription("No trace available for the given traceId")
            .asRuntimeException()
        )
    }
  }

}
