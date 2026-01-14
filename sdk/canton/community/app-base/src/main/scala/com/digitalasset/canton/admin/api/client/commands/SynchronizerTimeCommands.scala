// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  CustomClientTimeout,
  TimeoutType,
}
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.admin.v30
import com.digitalasset.canton.time.admin.v30.SynchronizerTimeServiceGrpc.SynchronizerTimeServiceStub
import com.digitalasset.canton.time.{FetchTimeResponse, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.Synchronizer
import io.grpc.ManagedChannel

import scala.concurrent.Future

object SynchronizerTimeCommands {

  abstract class BaseSynchronizerTimeCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = SynchronizerTimeServiceStub
    override def createService(channel: ManagedChannel): SynchronizerTimeServiceStub =
      v30.SynchronizerTimeServiceGrpc.stub(channel)
  }

  final case class FetchTimeRequest(
      synchronizerO: Option[Synchronizer],
      freshnessBound: NonNegativeFiniteDuration,
  ) {
    def toProtoV30: v30.FetchTimeRequest =
      v30.FetchTimeRequest(
        synchronizerO.map(_.toProtoV30),
        freshnessBound.toProtoPrimitive.some,
      )
  }

  final case class FetchTime(
      synchronizerO: Option[Synchronizer],
      freshnessBound: NonNegativeFiniteDuration,
      timeout: NonNegativeDuration,
  ) extends BaseSynchronizerTimeCommand[
        FetchTimeRequest,
        v30.FetchTimeResponse,
        FetchTimeResponse,
      ] {

    override protected def createRequest(): Either[String, FetchTimeRequest] =
      Right(FetchTimeRequest(synchronizerO, freshnessBound))

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

  final case class AwaitTimeRequest(
      synchronizerO: Option[Synchronizer],
      timestamp: CantonTimestamp,
  ) {
    def toProtoV30: v30.AwaitTimeRequest =
      v30.AwaitTimeRequest(synchronizerO.map(_.toProtoV30), timestamp.toProtoTimestamp.some)
  }

  final case class AwaitTime(
      synchronizer: Option[Synchronizer],
      time: CantonTimestamp,
      timeout: NonNegativeDuration,
  ) extends BaseSynchronizerTimeCommand[AwaitTimeRequest, v30.AwaitTimeResponse, Unit] {

    override protected def createRequest(): Either[String, AwaitTimeRequest] =
      Right(AwaitTimeRequest(synchronizer, time))

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
