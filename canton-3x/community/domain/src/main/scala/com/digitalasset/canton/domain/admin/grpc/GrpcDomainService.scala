// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.grpc

import cats.syntax.traverse.*
import com.digitalasset.canton.domain.admin.v0.GetDomainParameters.Response.Parameters
import com.digitalasset.canton.domain.admin.v0 as adminproto
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.google.protobuf.empty.Empty

import scala.concurrent.{ExecutionContext, Future}

class GrpcDomainService(
    staticDomainParameters: StaticDomainParameters,
    agreementManager: Option[ServiceAgreementManager],
)(implicit val ec: ExecutionContext)
    extends adminproto.DomainServiceGrpc.DomainService {

  override def listServiceAgreementAcceptances(
      request: Empty
  ): Future[adminproto.ServiceAgreementAcceptances] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    agreementManager
      .traverse { manager =>
        for {
          acceptances <- EitherTUtil.toFuture(CantonGrpcUtil.mapErr(manager.listAcceptances()))
        } yield adminproto.ServiceAgreementAcceptances(acceptances = acceptances.map(_.toProtoV0))
      }
      .map(_.getOrElse(adminproto.ServiceAgreementAcceptances(Seq())))
  }

  override def getDomainParametersVersioned(
      request: adminproto.GetDomainParameters.Request
  ): Future[adminproto.GetDomainParameters.Response] = {
    val response = staticDomainParameters.protoVersion.v match {
      case 1 => Future.successful(Parameters.ParametersV1(staticDomainParameters.toProtoV1))
      case unsupported =>
        Future.failed(
          new IllegalStateException(
            s"Unsupported Proto version $unsupported for static domain parameters"
          )
        )
    }

    response.map(adminproto.GetDomainParameters.Response(_))
  }
}
