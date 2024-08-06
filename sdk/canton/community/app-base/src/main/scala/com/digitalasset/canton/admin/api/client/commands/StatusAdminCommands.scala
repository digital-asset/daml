// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import ch.qos.logback.classic.Level
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.api.client.data.NodeStatus
import com.digitalasset.canton.health.admin.v0
import com.digitalasset.canton.health.admin.v0.{
  HealthDumpChunk,
  HealthDumpRequest,
  StatusServiceGrpc,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.empty.Empty
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ManagedChannel, Status}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object StatusAdminCommands {

  /* For backward compatibility:
  Assumes getting a left of gRPC code `Status.Code.UNIMPLEMENTED.type` means
  that the node specific status endpoint is not available (because that Canton
  version does not include it), and thus we want to fall back to the original
  status command in the caller.
   */
  abstract class NodeStatusCommand[
      S <: NodeStatus.Status,
      GrpcReq,
      GrpcResponse,
  ]()(implicit ec: ExecutionContext)
      extends GrpcAdminCommand[
        GrpcReq,
        Either[Status.Code.UNIMPLEMENTED.type, GrpcResponse],
        Either[Status.Code.UNIMPLEMENTED.type, NodeStatus[S]],
      ] {

    def getStatus(service: Svc, request: GrpcReq): Future[GrpcResponse]

    def submitReq(
        service: Svc,
        request: GrpcReq,
    ): Future[Either[Status.Code.UNIMPLEMENTED.type, GrpcResponse]] =
      getStatus(service, request)
        .transformWith {
          case Failure(exception: io.grpc.StatusRuntimeException)
              if exception.getStatus.getCode == Status.Code.UNIMPLEMENTED =>
            Future.successful(Status.Code.UNIMPLEMENTED.asLeft[GrpcResponse])

          case Failure(exception) => Future.failed(exception)

          case Success(response) =>
            Future.successful(response.asRight[Status.Code.UNIMPLEMENTED.type])
        }
  }

  abstract class StatusServiceCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
    override type Svc = v0.StatusServiceGrpc.StatusServiceStub
    override def createService(channel: ManagedChannel): v0.StatusServiceGrpc.StatusServiceStub =
      v0.StatusServiceGrpc.stub(channel)
  }

  abstract class GetStatusBase[Result] extends StatusServiceCommand[Empty, v0.NodeStatus, Result] {
    override def createRequest(): Either[String, Empty] = Right(Empty())
    override def submitRequest(
        service: v0.StatusServiceGrpc.StatusServiceStub,
        request: Empty,
    ): Future[v0.NodeStatus] =
      service.status(request)
  }

  class GetStatus[S <: NodeStatus.Status](
      deserialize: v0.NodeStatus.Status => ParsingResult[S]
  ) extends GetStatusBase[NodeStatus[S]] {
    override def handleResponse(response: v0.NodeStatus): Either[String, NodeStatus[S]] =
      ((response.response match {
        case v0.NodeStatus.Response.NotInitialized(notInitialized) =>
          Right(NodeStatus.NotInitialized(notInitialized.active))
        case v0.NodeStatus.Response.Success(status) =>
          deserialize(status).map(NodeStatus.Success(_))
        case v0.NodeStatus.Response.Empty => Left(ProtoDeserializationError.FieldNotSet("response"))
      }): ParsingResult[NodeStatus[S]]).leftMap(_.toString)
  }

  class GetHealthDump(
      observer: StreamObserver[HealthDumpChunk],
      chunkSize: Option[Int],
  ) extends GrpcAdminCommand[HealthDumpRequest, CancellableContext, CancellableContext] {
    override type Svc = v0.StatusServiceGrpc.StatusServiceStub
    override def createService(channel: ManagedChannel): v0.StatusServiceGrpc.StatusServiceStub =
      v0.StatusServiceGrpc.stub(channel)
    override def submitRequest(
        service: v0.StatusServiceGrpc.StatusServiceStub,
        request: HealthDumpRequest,
    ): Future[CancellableContext] = {
      val context = Context.current().withCancellation()
      context.run(() => service.healthDump(request, observer))
      Future.successful(context)
    }
    override def createRequest(): Either[String, HealthDumpRequest] = Right(
      HealthDumpRequest(chunkSize)
    )
    override def handleResponse(response: CancellableContext): Either[String, CancellableContext] =
      Right(response)

    override def timeoutType: GrpcAdminCommand.TimeoutType =
      GrpcAdminCommand.DefaultUnboundedTimeout

  }

  object IsRunning
      extends StatusAdminCommands.FromStatus({
        case v0.NodeStatus.Response.Empty => false
        case _ => true
      })

  object IsInitialized
      extends StatusAdminCommands.FromStatus({
        case v0.NodeStatus.Response.Success(_) => true
        case _ => false
      })

  class FromStatus(predicate: v0.NodeStatus.Response => Boolean) extends GetStatusBase[Boolean] {
    override def handleResponse(response: v0.NodeStatus): Either[String, Boolean] =
      (response.response match {
        case v0.NodeStatus.Response.Empty => Left(ProtoDeserializationError.FieldNotSet("response"))
        case other => Right(predicate(other))
      }).leftMap(_.toString)
  }

  class SetLogLevel(level: Level)
      extends StatusServiceCommand[v0.SetLogLevelRequest, v0.SetLogLevelResponse, Unit] {

    override def submitRequest(
        service: StatusServiceGrpc.StatusServiceStub,
        request: v0.SetLogLevelRequest,
    ): Future[v0.SetLogLevelResponse] = service.setLogLevel(request)

    override def createRequest(): Either[String, v0.SetLogLevelRequest] = Right(
      v0.SetLogLevelRequest(level.toString)
    )

    override def handleResponse(response: v0.SetLogLevelResponse): Either[String, Unit] = Right(())
  }

  class GetLastErrors()
      extends StatusServiceCommand[
        v0.GetLastErrorsRequest,
        v0.GetLastErrorsResponse,
        Map[String, String],
      ] {

    override def submitRequest(
        service: StatusServiceGrpc.StatusServiceStub,
        request: v0.GetLastErrorsRequest,
    ): Future[v0.GetLastErrorsResponse] =
      service.getLastErrors(request)
    override def createRequest(): Either[String, v0.GetLastErrorsRequest] = Right(
      v0.GetLastErrorsRequest()
    )
    override def handleResponse(
        response: v0.GetLastErrorsResponse
    ): Either[String, Map[String, String]] =
      response.errors.map(r => (r.traceId -> r.message)).toMap.asRight
  }

  class GetLastErrorTrace(traceId: String)
      extends StatusServiceCommand[v0.GetLastErrorTraceRequest, v0.GetLastErrorTraceResponse, Seq[
        String
      ]] {

    override def submitRequest(
        service: StatusServiceGrpc.StatusServiceStub,
        request: v0.GetLastErrorTraceRequest,
    ): Future[v0.GetLastErrorTraceResponse] =
      service.getErrorTrace(request)

    override def createRequest(): Either[String, v0.GetLastErrorTraceRequest] = Right(
      v0.GetLastErrorTraceRequest(traceId)
    )

    override def handleResponse(
        response: v0.GetLastErrorTraceResponse
    ): Either[String, Seq[String]] = Right(response.messages)
  }

}
