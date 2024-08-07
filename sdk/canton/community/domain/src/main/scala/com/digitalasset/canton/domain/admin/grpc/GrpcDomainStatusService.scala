// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.grpc

import com.digitalasset.canton.domain.admin.data.DomainStatus
import com.digitalasset.canton.domain.admin.v0.{DomainStatusRequest, DomainStatusResponse}
import com.digitalasset.canton.domain.admin.v0 as domainV0
import com.digitalasset.canton.health.NodeStatus
import com.digitalasset.canton.health.admin.{v0, v1}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}

import scala.concurrent.Future

class GrpcDomainStatusService(
    status: => NodeStatus[DomainStatus],
    val loggerFactory: NamedLoggerFactory,
) extends domainV0.DomainStatusServiceGrpc.DomainStatusService
    with NamedLogging {

  override def domainStatus(request: DomainStatusRequest): Future[DomainStatusResponse] = {

    val responseP: domainV0.DomainStatusResponse.Kind = status match {
      case NodeStatus.Failure(msg) =>
        domainV0.DomainStatusResponse.Kind.Failure(v1.Failure(msg))

      case NodeStatus.NotInitialized(active) =>
        domainV0.DomainStatusResponse.Kind.Unavailable(v0.NodeStatus.NotInitialized(active))

      case NodeStatus.Success(status: DomainStatus) =>
        domainV0.DomainStatusResponse.Kind.Status(status.toDomainStatusProto)
    }

    Future.successful(domainV0.DomainStatusResponse(responseP))
  }
}
