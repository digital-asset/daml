// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functor.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.TimestampSelector
import com.digitalasset.canton.domain.sequencing.sequencer.{OnboardingStateForSequencer, Sequencer}
import com.digitalasset.canton.error.{BaseCantonError, CantonError}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencer.admin.v30.OnboardingStateRequest.Request
import com.digitalasset.canton.sequencer.admin.v30.{
  OnboardingStateResponse,
  SetTrafficPurchasedRequest,
  SetTrafficPurchasedResponse,
}
import com.digitalasset.canton.sequencing.client.SequencerClientSend
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.{
  Member,
  SequencerId,
  TopologyManagerError,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils}
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}

import java.io.OutputStream
import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerAdministrationService(
    sequencer: Sequencer,
    sequencerClient: SequencerClientSend,
    topologyStore: TopologyStore[DomainStore],
    topologyClient: DomainTopologyClient,
    domainTimeTracker: DomainTimeTracker,
    staticDomainParameters: StaticDomainParameters,
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

    TimestampSelector.fromProtoV30(request.timestampSelector) match {
      case Left(err) =>
        Future.failed(
          new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(err.toString))
        )
      case Right(selector) =>
        val response = sequencer
          .trafficStatus(members, selector)
          .flatMap { states =>
            val (errors, trafficStates) = states.trafficStatesOrErrors.partitionMap {
              case (member, trafficStateE) =>
                trafficStateE
                  .map(member -> _)
                  .leftMap(member -> _)
            }
            if (errors.nonEmpty) {
              val errorMessage = errors.mkShow().toString
              FutureUnlessShutdown.failed(
                io.grpc.Status.INTERNAL
                  .withDescription(
                    s"Failed to retrieve traffic state for some members: $errorMessage"
                  )
                  .asRuntimeException()
              )
            } else {
              FutureUnlessShutdown.pure(
                trafficStates.map { case (member, state) =>
                  member.toProtoPrimitive -> state.toProtoV30
                }.toMap
              )
            }
          }
          .map(v30.TrafficControlStateResponse(_))

        CantonGrpcUtil.mapErrNewEUS(EitherT.right(response))
    }
  }

  override def snapshot(request: v30.SnapshotRequest): Future[v30.SnapshotResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    (for {
      timestamp <- wrapErr(
        ProtoConverter
          .parseRequired(CantonTimestamp.fromProtoTimestamp, "timestamp", request.timestamp)
      )
      result <- sequencer.snapshot(timestamp).leftWiden[BaseCantonError]
    } yield result)
      .fold[v30.SnapshotResponse](
        error =>
          v30.SnapshotResponse(
            v30.SnapshotResponse.Value.Failure(v30.SnapshotResponse.Failure(error.cause))
          ),
        result =>
          v30.SnapshotResponse(
            v30.SnapshotResponse.Value.VersionedSuccess(
              v30.SnapshotResponse.VersionedSuccess(result.toProtoVersioned.toByteString)
            )
          ),
      )
  }

  override def onboardingState(
      request: v30.OnboardingStateRequest,
      responseObserver: StreamObserver[OnboardingStateResponse],
  ): Unit =
    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => onboardingState(request, out),
      responseObserver,
      byteString => OnboardingStateResponse(byteString),
    )

  private def onboardingState(
      request: v30.OnboardingStateRequest,
      out: OutputStream,
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val parseMemberOrTimestamp = request.request match {
      case Request.Empty => Left(FieldNotSet("sequencer_id"): ProtoDeserializationError)
      case Request.SequencerUid(sequencerUid) =>
        UniqueIdentifier
          .fromProtoPrimitive(sequencerUid, "sequencer_id")
          .map(SequencerId(_))
          .map(Left(_))

      case Request.Timestamp(referenceEffectiveTime) =>
        CantonTimestamp.fromProtoTimestamp(referenceEffectiveTime).map(Right(_))
    }
    val res = for {
      memberOrTimestamp <- wrapErr(parseMemberOrTimestamp)
      referenceEffective <- memberOrTimestamp match {
        case Left(sequencerId) =>
          EitherT(
            topologyStore
              .findFirstSequencerStateForSequencer(sequencerId)
              .map(txOpt =>
                txOpt
                  .map(stored => stored.validFrom)
                  .toRight(
                    TopologyManagerError.InternalError
                      .Other(s"Did not find onboarding topology transaction for $sequencerId")
                  )
              )
          )
        case Right(timestamp) =>
          EitherT.rightT[Future, BaseCantonError](EffectiveTime(timestamp))
      }

      _ <- domainTimeTracker
        .awaitTick(referenceEffective.value)
        .map(EitherT.right[CantonError](_).void)
        .getOrElse(EitherTUtil.unit[BaseCantonError])

      /* find the sequencer snapshot that contains a sequenced timestamp that is >= to the reference/onboarding effective time
       if we take the sequencing time here, we might miss out topology transactions between sequencerSnapshot.lastTs and effectiveTime
       in the following scenario:
        t0: onboarding sequenced time
        t1: sequencerSnapshot.lastTs
        t2: sequenced time of some topology transaction
        t3: onboarding effective time

        Therefore, if we find the sequencer snapshot that "contains" the onboarding effective time,
        and we then use this snapshot's lastTs as the reference sequenced time for fetching the topology snapshot,
        we can be sure that
        a) the topology snapshot contains all topology transactions sequenced up to including the onboarding effective time
        b) the topology snapshot might contain a few more transactions between the onboarding effective time and the last sequenced time in the block
        c) the sequencer snapshot will contain the correct counter for the onboarding sequencer
        d) the onboarding sequencer will properly subscribe from its own minimum counter that it gets initialized with from the sequencer snapshot
       */

      sequencerSnapshot <- sequencer.snapshot(referenceEffective.value)

      topologySnapshot <- EitherT.right[BaseCantonError](
        topologyStore.findEssentialStateAtSequencedTime(SequencedTime(sequencerSnapshot.lastTs))
      )
    } yield OnboardingStateForSequencer(
      topologySnapshot,
      staticDomainParameters,
      sequencerSnapshot,
      staticDomainParameters.protocolVersion,
    ).toByteString.writeTo(out)

    mapErrNew(res)
  }

  override def disableMember(
      requestP: v30.DisableMemberRequest
  ): Future[v30.DisableMemberResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherTUtil.toFuture[StatusRuntimeException, v30.DisableMemberResponse] {
      for {
        member <- EitherT.fromEither[Future](
          Member
            .fromProtoPrimitive(requestP.member, "member")
            .leftMap(err =>
              new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(err.toString))
            )
        )
        _ <- sequencer.disableMember(member).leftMap(_.asGrpcError)
      } yield v30.DisableMemberResponse()
    }
  }

  /** Update the traffic purchased entry of a member
    * The top up will only become valid once authorized by all sequencers of the domain
    */
  override def setTrafficPurchased(
      requestP: SetTrafficPurchasedRequest
  ): Future[SetTrafficPurchasedResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {
      member <- wrapErrUS(Member.fromProtoPrimitive(requestP.member, "member"))
      serial <- wrapErrUS(ProtoConverter.parsePositiveInt("serial", requestP.serial))
      totalTrafficPurchased <- wrapErrUS(
        ProtoConverter.parseNonNegativeLong(
          "total_traffic_purchased",
          requestP.totalTrafficPurchased,
        )
      )
      _ <- sequencer
        .setTrafficPurchased(
          member,
          serial,
          totalTrafficPurchased,
          sequencerClient,
          domainTimeTracker,
        )
        .leftWiden[CantonError]
    } yield SetTrafficPurchasedResponse()

    mapErrNewEUS(result)
  }
}
