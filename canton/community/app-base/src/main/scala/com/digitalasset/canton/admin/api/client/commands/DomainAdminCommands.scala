// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import com.digitalasset.canton.admin.api.client.data.StaticDomainParameters as StaticDomainParametersConfig
import com.digitalasset.canton.domain.admin.v30 as adminproto
import com.digitalasset.canton.protocol.StaticDomainParameters as StaticDomainParametersInternal
import io.grpc.ManagedChannel

import scala.concurrent.Future

object DomainAdminCommands {

  abstract class BaseDomainServiceCommand[Req, Rep, Res] extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = adminproto.DomainServiceGrpc.DomainServiceStub
    override def createService(
        channel: ManagedChannel
    ): adminproto.DomainServiceGrpc.DomainServiceStub =
      adminproto.DomainServiceGrpc.stub(channel)
  }

  final case class GetDomainParameters()
      extends BaseDomainServiceCommand[
        adminproto.GetDomainParameters.Request,
        adminproto.GetDomainParameters.Response,
        StaticDomainParametersConfig,
      ] {
    override def createRequest(): Either[String, adminproto.GetDomainParameters.Request] = Right(
      adminproto.GetDomainParameters.Request()
    )
    override def submitRequest(
        service: adminproto.DomainServiceGrpc.DomainServiceStub,
        request: adminproto.GetDomainParameters.Request,
    ): Future[adminproto.GetDomainParameters.Response] =
      service.getDomainParametersVersioned(adminproto.GetDomainParameters.Request())

    override def handleResponse(
        response: adminproto.GetDomainParameters.Response
    ): Either[String, StaticDomainParametersConfig] = {
      import adminproto.GetDomainParameters.Response.Parameters

      response.parameters match {
        case Parameters.Empty => Left("Field parameters was not found in the response")
        case Parameters.ParametersV1(parametersV1) =>
          (for {
            staticDomainParametersInternal <- StaticDomainParametersInternal.fromProtoV30(
              parametersV1
            )
            staticDomainParametersConfig = StaticDomainParametersConfig(
              staticDomainParametersInternal
            )
          } yield staticDomainParametersConfig).leftMap(_.toString)
      }
    }
  }
}
