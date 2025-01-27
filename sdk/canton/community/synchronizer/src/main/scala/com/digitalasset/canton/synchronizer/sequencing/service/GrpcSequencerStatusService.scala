// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import com.digitalasset.canton.admin.sequencer.v30 as sequencerV30
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.admin.data.SequencerNodeStatus
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.Future

class GrpcSequencerStatusService(
    status: => NodeStatus[SequencerNodeStatus],
    val loggerFactory: NamedLoggerFactory,
) extends sequencerV30.SequencerStatusServiceGrpc.SequencerStatusService
    with NamedLogging {

  override def sequencerStatus(
      request: sequencerV30.SequencerStatusRequest
  ): Future[sequencerV30.SequencerStatusResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val responseP: sequencerV30.SequencerStatusResponse.Kind = status match {
      case NodeStatus.Failure(_msg) =>
        logger.warn(s"Unexpectedly found failure status: ${_msg}")
        sequencerV30.SequencerStatusResponse.Kind.Empty

      case notInitialized: NodeStatus.NotInitialized =>
        sequencerV30.SequencerStatusResponse.Kind.NotInitialized(notInitialized.toProtoV30)

      case NodeStatus.Success(status: SequencerNodeStatus) =>
        sequencerV30.SequencerStatusResponse.Kind.Status(status.toSequencerStatusProto)
    }

    Future.successful(sequencerV30.SequencerStatusResponse(responseP))
  }
}
