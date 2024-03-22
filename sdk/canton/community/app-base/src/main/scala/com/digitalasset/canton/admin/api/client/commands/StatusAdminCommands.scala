// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.health.admin.v0.{HealthDumpChunk, HealthDumpRequest}
import com.digitalasset.canton.health.admin.{data, v0}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.empty.Empty
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ManagedChannel}

import scala.concurrent.Future

object StatusAdminCommands {
  abstract class GetStatusBase[Result] extends GrpcAdminCommand[Empty, v0.NodeStatus, Result] {
    override type Svc = v0.StatusServiceGrpc.StatusServiceStub
    override def createService(channel: ManagedChannel): v0.StatusServiceGrpc.StatusServiceStub =
      v0.StatusServiceGrpc.stub(channel)
    override def createRequest(): Either[String, Empty] = Right(Empty())
    override def submitRequest(
        service: v0.StatusServiceGrpc.StatusServiceStub,
        request: Empty,
    ): Future[v0.NodeStatus] =
      service.status(request)
  }

  class GetStatus[S <: data.NodeStatus.Status](
      deserialize: v0.NodeStatus.Status => ParsingResult[S]
  ) extends GetStatusBase[data.NodeStatus[S]] {
    override def handleResponse(response: v0.NodeStatus): Either[String, data.NodeStatus[S]] =
      ((response.response match {
        case v0.NodeStatus.Response.NotInitialized(notInitialized) =>
          Right(data.NodeStatus.NotInitialized(notInitialized.active))
        case v0.NodeStatus.Response.Success(status) =>
          deserialize(status).map(data.NodeStatus.Success(_))
        case v0.NodeStatus.Response.Empty => Left(ProtoDeserializationError.FieldNotSet("response"))
      }): ParsingResult[data.NodeStatus[S]]).leftMap(_.toString)
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
}
