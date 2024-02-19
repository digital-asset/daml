// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v30
import com.digitalasset.canton.domain.admin.v30.{
  SetTrafficBalanceRequest,
  SetTrafficBalanceResponse,
}
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.{Status, StatusException}

import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerAdministrationService(
    sequencer: Sequencer,
    sequencerClient: SequencerClient,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends v30.SequencerAdministrationServiceGrpc.SequencerAdministrationService
    with NamedLogging {

  override def pruningStatus(
      request: v30.PruningStatusRequest
  ): Future[v30.PruningStatusResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    sequencer.pruningStatus.map(_.toProtoV30).map(status => v30.PruningStatusResponse(Some(status)))
  }

  override def trafficControlState(
      request: v30.TrafficControlStateRequest
  ): Future[v30.TrafficControlStateResponse] = {
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
        _.members.map(_.toProtoV30)
      }
      .map(
        v30.TrafficControlStateResponse(_)
      )
  }

  override def snapshot(request: v30.SnapshotRequest): Future[v30.SnapshotResponse] = {
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
      .fold[v30.SnapshotResponse](
        error =>
          v30.SnapshotResponse(
            v30.SnapshotResponse.Value.Failure(v30.SnapshotResponse.Failure(error))
          ),
        result =>
          v30.SnapshotResponse(
            v30.SnapshotResponse.Value.VersionedSuccess(
              v30.SnapshotResponse.VersionedSuccess(result.toProtoVersioned.toByteString)
            )
          ),
      )
  }

  override def disableMember(
      requestP: v30.DisableMemberRequest
  ): Future[v30.DisableMemberResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherTUtil.toFuture[StatusException, v30.DisableMemberResponse] {
      for {
        member <- EitherT.fromEither[Future](
          Member
            .fromProtoPrimitive(requestP.member, "member")
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString).asException())
        )
        _ <- EitherT.right(sequencer.disableMember(member))
      } yield v30.DisableMemberResponse()
    }
  }

  /** Update the traffic balance of a member
    * The top up will only become valid once authorized by all sequencers of the domain
    */
  override def setTrafficBalance(
      requestP: SetTrafficBalanceRequest
  ): Future[
    SetTrafficBalanceResponse
  ] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = {
      for {
        member <- wrapErrUS(Member.fromProtoPrimitive(requestP.member, "member"))
        serial <- wrapErrUS(ProtoConverter.parseNonNegativeLong(requestP.serial))
        totalTrafficBalance <- wrapErrUS(
          ProtoConverter.parseNonNegativeLong(requestP.totalTrafficBalance)
        )
        highestMaxSequencingTimestamp <- sequencer
          .setTrafficBalance(member, serial, totalTrafficBalance, sequencerClient)
          .leftWiden[CantonError]
      } yield SetTrafficBalanceResponse(
        maxSequencingTimestamp = Some(highestMaxSequencingTimestamp.toProtoPrimitive)
      )
    }

    mapErrNewEUS(result)
  }
}
