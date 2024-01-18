// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.domain.admin.v0.{
  TrafficControlStateRequest,
  TrafficControlStateResponse,
}
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.google.protobuf.empty.Empty
import io.grpc.{Status, StatusException}

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

  override def snapshot(request: v0.Snapshot.Request): Future[v0.Snapshot.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    (for {
      timestamp <- EitherT
        .fromEither[Future](
          ProtoConverter
            .parseRequired(CantonTimestamp.fromProtoPrimitive, "timestamp", request.timestamp)
        )
        .leftMap(_.toString)
      result <- sequencer.snapshot(timestamp)
    } yield result)
      .fold[v0.Snapshot.Response](
        error =>
          v0.Snapshot.Response(v0.Snapshot.Response.Value.Failure(v0.Snapshot.Failure(error))),
        result =>
          v0.Snapshot.Response(
            v0.Snapshot.Response.Value.VersionedSuccess(
              v0.Snapshot.VersionedSuccess(result.toProtoVersioned.toByteString)
            )
          ),
      )
  }

  override def disableMember(requestP: v0.DisableMemberRequest): Future[Empty] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherTUtil.toFuture[StatusException, Empty] {
      for {
        member <- EitherT.fromEither[Future](
          Member
            .fromProtoPrimitive(requestP.member, "member")
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString).asException())
        )
        _ <- EitherT.right(sequencer.disableMember(member))
      } yield Empty()
    }
  }
}
