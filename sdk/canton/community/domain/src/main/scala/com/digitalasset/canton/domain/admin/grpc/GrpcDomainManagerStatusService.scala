// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.grpc

import com.digitalasset.canton.domain.admin.data.DomainManagerNodeStatus
import com.digitalasset.canton.domain.admin.v0.{
  DomainManagerStatusRequest,
  DomainManagerStatusResponse,
}
import com.digitalasset.canton.domain.admin.v0 as domainV0
import com.digitalasset.canton.health.NodeStatus
import com.digitalasset.canton.health.admin.{v0, v1}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}

import scala.concurrent.Future

class GrpcDomainManagerStatusService(
    status: => NodeStatus[DomainManagerNodeStatus],
    val loggerFactory: NamedLoggerFactory,
) extends domainV0.DomainManagerStatusServiceGrpc.DomainManagerStatusService
    with NamedLogging {

  override def domainManagerStatus(
      request: DomainManagerStatusRequest
  ): Future[DomainManagerStatusResponse] = {
    val responseP: domainV0.DomainManagerStatusResponse.Kind = status match {
      case NodeStatus.Failure(msg) =>
        domainV0.DomainManagerStatusResponse.Kind.Failure(v1.Failure(msg))

      case NodeStatus.NotInitialized(active) =>
        domainV0.DomainManagerStatusResponse.Kind.Unavailable(v0.NodeStatus.NotInitialized(active))

      case NodeStatus.Success(status: DomainManagerNodeStatus) =>
        domainV0.DomainManagerStatusResponse.Kind.Status(status.toDomainManagerStatusProto)
    }

    Future.successful(domainV0.DomainManagerStatusResponse(responseP))

  }

}
