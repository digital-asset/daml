// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import com.digitalasset.canton.admin.api.client.data.StaticDomainParameters as ConsoleStaticDomainParameters
import com.digitalasset.canton.domain.api.v30.SequencerConnect.GetDomainParametersResponse.Parameters
import com.digitalasset.canton.domain.api.v30.SequencerConnectServiceGrpc.SequencerConnectServiceStub
import com.digitalasset.canton.domain.api.v30 as proto
import com.digitalasset.canton.topology.DomainId
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

  final case object GetDomainId
      extends SequencerConnectServiceCommands[
        Empty,
        proto.SequencerConnect.GetDomainIdResponse,
        DomainId,
      ] {
    override def createRequest(): Either[String, Empty] = Right(Empty())

    override def submitRequest(
        service: SequencerConnectServiceStub,
        request: Empty,
    ): Future[proto.SequencerConnect.GetDomainIdResponse] =
      service.getDomainId(proto.SequencerConnect.GetDomainIdRequest())

    override def handleResponse(
        response: proto.SequencerConnect.GetDomainIdResponse
    ): Either[String, DomainId] = {

      DomainId.fromProtoPrimitive(response.domainId, "domain_id").leftMap(_.message)
    }
  }

  final case object GetStaticDomainParameters
      extends SequencerConnectServiceCommands[
        Empty,
        proto.SequencerConnect.GetDomainParametersResponse,
        ConsoleStaticDomainParameters,
      ] {
    override def createRequest(): Either[String, Empty] = Right(Empty())

    override def submitRequest(
        service: SequencerConnectServiceStub,
        request: Empty,
    ): Future[proto.SequencerConnect.GetDomainParametersResponse] =
      service.getDomainParameters(proto.SequencerConnect.GetDomainParametersRequest())

    override def handleResponse(
        response: proto.SequencerConnect.GetDomainParametersResponse
    ): Either[String, ConsoleStaticDomainParameters] = {

      response.parameters match {
        case Parameters.Empty => Left("Domain parameters should not be empty")
        case Parameters.ParametersV1(value) =>
          ConsoleStaticDomainParameters.fromProtoV30(value).leftMap(_.message)
      }
    }
  }

}
