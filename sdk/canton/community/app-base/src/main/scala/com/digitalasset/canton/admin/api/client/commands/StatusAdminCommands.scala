// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import ch.qos.logback.classic.Level
import com.digitalasset.canton.admin.api.client.data.{NodeStatus, WaitingForExternalInput}
import com.digitalasset.canton.admin.health.v30
import com.digitalasset.canton.admin.health.v30.{
  HealthDumpRequest,
  HealthDumpResponse,
  StatusServiceGrpc,
}
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ManagedChannel}

import scala.concurrent.Future

object StatusAdminCommands {

  abstract class NodeStatusCommand[
      S <: NodeStatus.Status,
      GrpcReq,
      GrpcResponse,
  ] extends GrpcAdminCommand[GrpcReq, GrpcResponse, NodeStatus[S]] {
    def createRequest(): Either[String, GrpcReq]

    def getStatus(service: Svc, request: GrpcReq): Future[GrpcResponse]

    def submitReq(
        service: Svc,
        request: GrpcReq,
    ): Future[GrpcResponse] = getStatus(service, request)
  }

  /** Query the shared part of the status endpoint and project to an attribute
    * @param cmd Comment to query the node status endpoint
    * @param projector Projector from the node status to the attribute
    */
  final case class NodeStatusElement[S <: NodeStatus.Status, GrpcReq, GrpcResponse, T](
      cmd: NodeStatusCommand[S, GrpcReq, GrpcResponse],
      projector: NodeStatus[NodeStatus.Status] => T,
  ) extends GrpcAdminCommand[GrpcReq, GrpcResponse, T] {
    override type Svc = cmd.Svc

    override def createService(channel: ManagedChannel): Svc = cmd.createService(channel)

    override def submitRequest(service: Svc, request: GrpcReq): Future[GrpcResponse] =
      cmd.submitRequest(service, request)

    override def createRequest(): Either[String, GrpcReq] = cmd.createRequest()

    override def handleResponse(response: GrpcResponse): Either[String, T] =
      cmd.handleResponse(response).map(projector)
  }

  object NodeStatusElement {
    def isWaitingForExternalInput(
        s: NodeStatus[NodeStatus.Status],
        kind: WaitingForExternalInput,
    ): Boolean =
      s match {
        case _: NodeStatus.Failure | _: NodeStatus.Success[?] => false
        case NodeStatus.NotInitialized(_active, waitingFor) => waitingFor.contains(kind)
      }
  }

  class GetHealthDump(
      observer: StreamObserver[HealthDumpResponse],
      chunkSize: Option[Int],
  ) extends GrpcAdminCommand[HealthDumpRequest, CancellableContext, CancellableContext] {
    override type Svc = v30.StatusServiceGrpc.StatusServiceStub
    override def createService(channel: ManagedChannel): v30.StatusServiceGrpc.StatusServiceStub =
      v30.StatusServiceGrpc.stub(channel)
    override def submitRequest(
        service: v30.StatusServiceGrpc.StatusServiceStub,
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

  abstract class StatusServiceCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
    override type Svc = v30.StatusServiceGrpc.StatusServiceStub

    override def createService(channel: ManagedChannel): v30.StatusServiceGrpc.StatusServiceStub =
      v30.StatusServiceGrpc.stub(channel)
  }

  class SetLogLevel(level: Level)
      extends StatusServiceCommand[v30.SetLogLevelRequest, v30.SetLogLevelResponse, Unit] {

    override def submitRequest(
        service: StatusServiceGrpc.StatusServiceStub,
        request: v30.SetLogLevelRequest,
    ): Future[v30.SetLogLevelResponse] = service.setLogLevel(request)

    override def createRequest(): Either[String, v30.SetLogLevelRequest] = Right(
      v30.SetLogLevelRequest(level.toString)
    )

    override def handleResponse(response: v30.SetLogLevelResponse): Either[String, Unit] = Right(())
  }

  class GetLastErrors()
      extends StatusServiceCommand[
        v30.GetLastErrorsRequest,
        v30.GetLastErrorsResponse,
        Map[String, String],
      ] {

    override def submitRequest(
        service: StatusServiceGrpc.StatusServiceStub,
        request: v30.GetLastErrorsRequest,
    ): Future[v30.GetLastErrorsResponse] =
      service.getLastErrors(request)
    override def createRequest(): Either[String, v30.GetLastErrorsRequest] = Right(
      v30.GetLastErrorsRequest()
    )
    override def handleResponse(
        response: v30.GetLastErrorsResponse
    ): Either[String, Map[String, String]] =
      response.errors.map(r => (r.traceId -> r.message)).toMap.asRight
  }

  class GetLastErrorTrace(traceId: String)
      extends StatusServiceCommand[v30.GetLastErrorTraceRequest, v30.GetLastErrorTraceResponse, Seq[
        String
      ]] {

    override def submitRequest(
        service: StatusServiceGrpc.StatusServiceStub,
        request: v30.GetLastErrorTraceRequest,
    ): Future[v30.GetLastErrorTraceResponse] =
      service.getLastErrorTrace(request)

    override def createRequest(): Either[String, v30.GetLastErrorTraceRequest] = Right(
      v30.GetLastErrorTraceRequest(traceId)
    )

    override def handleResponse(
        response: v30.GetLastErrorTraceResponse
    ): Either[String, Seq[String]] = Right(response.messages)
  }

}
