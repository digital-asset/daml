// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  CustomClientTimeout,
  TimeoutType,
}
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.admin.v30
import com.digitalasset.canton.time.admin.v30.DomainTimeServiceGrpc.DomainTimeServiceStub
import com.digitalasset.canton.time.{
  AwaitTimeRequest,
  FetchTimeRequest,
  FetchTimeResponse,
  NonNegativeFiniteDuration,
}
import com.digitalasset.canton.topology.DomainId
import io.grpc.ManagedChannel

import scala.concurrent.Future

object DomainTimeCommands {

  abstract class BaseDomainTimeCommand[Req, Rep, Res] extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = DomainTimeServiceStub
    override def createService(channel: ManagedChannel): DomainTimeServiceStub =
      v30.DomainTimeServiceGrpc.stub(channel)
  }

  final case class FetchTime(
      domainIdO: Option[DomainId],
      freshnessBound: NonNegativeFiniteDuration,
      timeout: NonNegativeDuration,
  ) extends BaseDomainTimeCommand[FetchTimeRequest, v30.FetchTimeResponse, FetchTimeResponse] {

    override def createRequest(): Either[String, FetchTimeRequest] =
      Right(FetchTimeRequest(domainIdO, freshnessBound))

    override def submitRequest(
        service: DomainTimeServiceStub,
        request: FetchTimeRequest,
    ): Future[v30.FetchTimeResponse] =
      service.fetchTime(request.toProtoV30)

    override def handleResponse(
        response: v30.FetchTimeResponse
    ): Either[String, FetchTimeResponse] =
      FetchTimeResponse.fromProto(response).leftMap(_.toString)

    override def timeoutType: TimeoutType = CustomClientTimeout(timeout)
  }

  final case class AwaitTime(
      domainIdO: Option[DomainId],
      time: CantonTimestamp,
      timeout: NonNegativeDuration,
  ) extends BaseDomainTimeCommand[AwaitTimeRequest, v30.AwaitTimeResponse, Unit] {

    override def createRequest(): Either[String, AwaitTimeRequest] =
      Right(AwaitTimeRequest(domainIdO, time))

    override def submitRequest(
        service: DomainTimeServiceStub,
        request: AwaitTimeRequest,
    ): Future[v30.AwaitTimeResponse] =
      service.awaitTime(request.toProtoV30)

    override def handleResponse(response: v30.AwaitTimeResponse): Either[String, Unit] = Right(())

    override def timeoutType: TimeoutType = CustomClientTimeout(timeout)
  }
}
