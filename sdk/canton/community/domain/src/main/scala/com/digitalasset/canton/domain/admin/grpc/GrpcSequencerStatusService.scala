// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.grpc

import com.digitalasset.canton.domain.admin.data.SequencerNodeStatus
import com.digitalasset.canton.domain.admin.v0 as domainV0
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.health.admin.{data, v0, v1}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}

import scala.concurrent.Future

class GrpcSequencerStatusService(
    status: => data.NodeStatus[SequencerNodeStatus],
    val loggerFactory: NamedLoggerFactory,
) extends domainV0.SequencerStatusServiceGrpc.SequencerStatusService
    with NamedLogging {

  override def sequencerStatus(
      request: domainV0.SequencerStatusRequest
  ): Future[domainV0.SequencerStatusResponse] = {

    val responseP: domainV0.SequencerStatusResponse.Kind = status match {
      case NodeStatus.Failure(msg) =>
        domainV0.SequencerStatusResponse.Kind.Failure(v1.Failure(msg))

      case NodeStatus.NotInitialized(active) =>
        domainV0.SequencerStatusResponse.Kind.Unavailable(v0.NodeStatus.NotInitialized(active))

      case NodeStatus.Success(status: SequencerNodeStatus) =>
        domainV0.SequencerStatusResponse.Kind.Status(status.toSequencerStatusProto)
    }

    Future.successful(domainV0.SequencerStatusResponse(responseP))
  }
}
