// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.time.admin.v30.SynchronizerTimeServiceGrpc.SynchronizerTimeServiceStub
import com.digitalasset.canton.time.{
  AwaitTimeRequest,
  FetchTimeRequest,
  FetchTimeResponse,
  NonNegativeFiniteDuration,
}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import io.grpc.ManagedChannel

import scala.concurrent.Future

object SynchronizerTimeCommands {

  abstract class BaseSynchronizerTimeCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = SynchronizerTimeServiceStub
    override def createService(channel: ManagedChannel): SynchronizerTimeServiceStub =
      v30.SynchronizerTimeServiceGrpc.stub(channel)
  }

  // TODO(#25483) All these commands should probably be logical with resolution on the server
  final case class FetchTime(
      synchronizerIdO: Option[PhysicalSynchronizerId],
      freshnessBound: NonNegativeFiniteDuration,
      timeout: NonNegativeDuration,
  ) extends BaseSynchronizerTimeCommand[
        FetchTimeRequest,
        v30.FetchTimeResponse,
        FetchTimeResponse,
      ] {

    override protected def createRequest(): Either[String, FetchTimeRequest] =
      Right(FetchTimeRequest(synchronizerIdO, freshnessBound))

    override protected def submitRequest(
        service: SynchronizerTimeServiceStub,
        request: FetchTimeRequest,
    ): Future[v30.FetchTimeResponse] =
      service.fetchTime(request.toProtoV30)

    override protected def handleResponse(
        response: v30.FetchTimeResponse
    ): Either[String, FetchTimeResponse] =
      FetchTimeResponse.fromProto(response).leftMap(_.toString)

    override def timeoutType: TimeoutType = CustomClientTimeout(timeout)
  }

  final case class AwaitTime(
      synchronizerIdO: Option[PhysicalSynchronizerId],
      time: CantonTimestamp,
      timeout: NonNegativeDuration,
  ) extends BaseSynchronizerTimeCommand[AwaitTimeRequest, v30.AwaitTimeResponse, Unit] {

    override protected def createRequest(): Either[String, AwaitTimeRequest] =
      Right(AwaitTimeRequest(synchronizerIdO, time))

    override protected def submitRequest(
        service: SynchronizerTimeServiceStub,
        request: AwaitTimeRequest,
    ): Future[v30.AwaitTimeResponse] =
      service.awaitTime(request.toProtoV30)

    override protected def handleResponse(response: v30.AwaitTimeResponse): Either[String, Unit] =
      Either.unit

    override def timeoutType: TimeoutType = CustomClientTimeout(timeout)
  }
}
