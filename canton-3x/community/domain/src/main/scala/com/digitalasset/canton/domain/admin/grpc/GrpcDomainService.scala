// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.grpc

import com.digitalasset.canton.domain.admin.v0.GetDomainParameters.Response.Parameters
import com.digitalasset.canton.domain.admin.v0 as adminproto
import com.digitalasset.canton.protocol.StaticDomainParameters

import scala.concurrent.{ExecutionContext, Future}

class GrpcDomainService(
    staticDomainParameters: StaticDomainParameters
)(implicit val ec: ExecutionContext)
    extends adminproto.DomainServiceGrpc.DomainService {

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
