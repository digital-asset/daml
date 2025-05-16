// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters as ConsoleStaticSynchronizerParameters
import com.digitalasset.canton.sequencer.api.v30 as proto
import com.digitalasset.canton.sequencer.api.v30.SequencerConnect.GetSynchronizerParametersResponse.Parameters
import com.digitalasset.canton.sequencer.api.v30.SequencerConnectServiceGrpc.SequencerConnectServiceStub
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import scala.concurrent.Future

object SequencerPublicCommands {

  abstract class SequencerConnectServiceCommands[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = SequencerConnectServiceStub

    override def createService(channel: ManagedChannel): SequencerConnectServiceStub =
      proto.SequencerConnectServiceGrpc.stub(channel)
  }

  final case object GetSynchronizerId
      extends SequencerConnectServiceCommands[
        Empty,
        proto.SequencerConnect.GetSynchronizerIdResponse,
        PhysicalSynchronizerId,
      ] {
    override protected def createRequest(): Either[String, Empty] = Right(Empty())

    override protected def submitRequest(
        service: SequencerConnectServiceStub,
        request: Empty,
    ): Future[proto.SequencerConnect.GetSynchronizerIdResponse] =
      service.getSynchronizerId(proto.SequencerConnect.GetSynchronizerIdRequest())

    override protected def handleResponse(
        response: proto.SequencerConnect.GetSynchronizerIdResponse
    ): Either[String, PhysicalSynchronizerId] =
      PhysicalSynchronizerId
        .fromProtoPrimitive(response.physicalSynchronizerId, "synchronizer_id")
        .leftMap(_.message)
  }

  final case object GetStaticSynchronizerParameters
      extends SequencerConnectServiceCommands[
        Empty,
        proto.SequencerConnect.GetSynchronizerParametersResponse,
        ConsoleStaticSynchronizerParameters,
      ] {
    override protected def createRequest(): Either[String, Empty] = Right(Empty())

    override protected def submitRequest(
        service: SequencerConnectServiceStub,
        request: Empty,
    ): Future[proto.SequencerConnect.GetSynchronizerParametersResponse] =
      service.getSynchronizerParameters(proto.SequencerConnect.GetSynchronizerParametersRequest())

    override protected def handleResponse(
        response: proto.SequencerConnect.GetSynchronizerParametersResponse
    ): Either[String, ConsoleStaticSynchronizerParameters] =
      response.parameters match {
        case Parameters.Empty => Left("Synchronizer parameters should not be empty")
        case Parameters.ParametersV1(value) =>
          ConsoleStaticSynchronizerParameters.fromProtoV30(value).leftMap(_.message)
      }
  }

}
