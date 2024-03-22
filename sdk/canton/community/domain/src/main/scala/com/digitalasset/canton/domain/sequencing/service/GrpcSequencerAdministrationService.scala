// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.syntax.either.*
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.domain.admin.v0.{
  TrafficControlStateRequest,
  TrafficControlStateResponse,
}
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.google.protobuf.empty.Empty

import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerAdministrationService(
    sequencer: Sequencer,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends v0.SequencerAdministrationServiceGrpc.SequencerAdministrationService
    with NamedLogging {

  override def pruningStatus(request: Empty): Future[v0.SequencerPruningStatus] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    sequencer.pruningStatus.map(_.toProtoV0)
  }

  override def trafficControlState(
      request: TrafficControlStateRequest
  ): Future[TrafficControlStateResponse] = {
    implicit val tc: TraceContext = TraceContextGrpc.fromGrpcContext

    def deserializeMember(memberP: String) =
      Member.fromProtoPrimitive(memberP, "member").map(Some(_)).valueOr { err =>
        logger.info(s"Cannot deserialized value to member: $err")
        None
      }

    val members = request.members.flatMap(deserializeMember)

    sequencer
      .trafficStatus(members)
      .map {
        _.members.map(_.toProtoV0)
      }
      .map(
        TrafficControlStateResponse(_)
      )
  }

}
