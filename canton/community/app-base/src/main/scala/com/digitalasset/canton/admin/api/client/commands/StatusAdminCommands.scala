// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.health.admin.v30.{HealthDumpRequest, HealthDumpResponse}
import com.digitalasset.canton.health.admin.{data, v30}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ManagedChannel}

import scala.concurrent.Future

object StatusAdminCommands {
  abstract class GetStatusBase[Result]
      extends GrpcAdminCommand[v30.StatusRequest, v30.StatusResponse, Result] {
    override type Svc = v30.StatusServiceGrpc.StatusServiceStub
    override def createService(channel: ManagedChannel): v30.StatusServiceGrpc.StatusServiceStub =
      v30.StatusServiceGrpc.stub(channel)
    override def createRequest(): Either[String, v30.StatusRequest] = Right(v30.StatusRequest())
    override def submitRequest(
        service: v30.StatusServiceGrpc.StatusServiceStub,
        request: v30.StatusRequest,
    ): Future[v30.StatusResponse] =
      service.status(request)
  }

  class GetStatus[S <: data.NodeStatus.Status](
      deserialize: v30.StatusResponse.Status => ParsingResult[S]
  ) extends GetStatusBase[data.NodeStatus[S]] {
    override def handleResponse(response: v30.StatusResponse): Either[String, data.NodeStatus[S]] =
      ((response.response match {
        case v30.StatusResponse.Response.NotInitialized(notInitialized) =>
          Right(data.NodeStatus.NotInitialized(notInitialized.active))
        case v30.StatusResponse.Response.Success(status) =>
          deserialize(status).map(data.NodeStatus.Success(_))
        case v30.StatusResponse.Response.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("response"))
      }): ParsingResult[data.NodeStatus[S]]).leftMap(_.toString)
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

  object IsRunning
      extends StatusAdminCommands.FromStatus({
        case v30.StatusResponse.Response.Empty => false
        case _ => true
      })

  object IsInitialized
      extends StatusAdminCommands.FromStatus({
        case v30.StatusResponse.Response.Success(_) => true
        case _ => false
      })

  class FromStatus(predicate: v30.StatusResponse.Response => Boolean)
      extends GetStatusBase[Boolean] {
    override def handleResponse(response: v30.StatusResponse): Either[String, Boolean] =
      (response.response match {
        case v30.StatusResponse.Response.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("response"))
        case other => Right(predicate(other))
      }).leftMap(_.toString)
  }
}
