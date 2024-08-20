// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import com.digitalasset.canton.admin.domain.v30 as domainV30
import com.digitalasset.canton.domain.sequencing.admin.data.SequencerNodeStatus
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.Future

class GrpcSequencerStatusService(
    status: => NodeStatus[SequencerNodeStatus],
    val loggerFactory: NamedLoggerFactory,
) extends domainV30.SequencerStatusServiceGrpc.SequencerStatusService
    with NamedLogging {

  override def sequencerStatus(
      request: domainV30.SequencerStatusRequest
  ): Future[domainV30.SequencerStatusResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val responseP: domainV30.SequencerStatusResponse.Kind = status match {
      case NodeStatus.Failure(_msg) =>
        logger.warn(s"Unexpectedly found failure status: ${_msg}")
        domainV30.SequencerStatusResponse.Kind.Empty

      case notInitialized: NodeStatus.NotInitialized =>
        domainV30.SequencerStatusResponse.Kind.NotInitialized(notInitialized.toProtoV30)

      case NodeStatus.Success(status: SequencerNodeStatus) =>
        domainV30.SequencerStatusResponse.Kind.Status(status.toSequencerStatusProto)
    }

    Future.successful(domainV30.SequencerStatusResponse(responseP))
  }
}
