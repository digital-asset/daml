// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.{OnboardingStateForSequencer, Sequencer}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencer.admin.v30.OnboardingStateRequest.Request
import com.digitalasset.canton.sequencer.admin.v30.{
  SetTrafficBalanceRequest,
  SetTrafficBalanceResponse,
}
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionX,
  StoredTopologyTransactionsX,
  TopologyStoreX,
}
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransactionX,
  TopologyChangeOpX,
  TopologyMappingX,
}
import com.digitalasset.canton.topology.{Member, SequencerId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerAdministrationService(
    sequencer: Sequencer,
    sequencerClient: SequencerClient,
    topologyStore: TopologyStoreX[DomainStore],
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

    val response = sequencer
      .trafficStatus(members)
      .map {
        _.members.map(_.toProtoV30)
      }
      .map(
        v30.TrafficControlStateResponse(_)
      )

    CantonGrpcUtil.mapErrNewEUS(EitherT.liftF(response))
  }

  override def snapshot(request: v30.SnapshotRequest): Future[v30.SnapshotResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    (for {
      timestamp <- EitherT
        .fromEither[Future](
          ProtoConverter
            .parseRequired(CantonTimestamp.fromProtoTimestamp, "timestamp", request.timestamp)
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

  override def onboardingState(
      request: v30.OnboardingStateRequest
  ): Future[v30.OnboardingStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val parseMemberOrTimestamp = request.request match {
      case Request.Empty => Left(FieldNotSet("sequencer_id"): ProtoDeserializationError)
      case Request.SequencerId(sequencerId) =>
        SequencerId
          .fromProtoPrimitive(sequencerId, "sequencer_id")
          .map(Left(_))

      case Request.Timestamp(referenceEffectiveTime) =>
        CantonTimestamp.fromProtoTimestamp(referenceEffectiveTime).map(Right(_))
    }
    (for {
      memberOrTimestamp <- EitherT.fromEither[Future](parseMemberOrTimestamp).leftMap(_.toString)
      referenceEffective <- memberOrTimestamp match {
        case Left(sequencerId) =>
          EitherT(
            topologyStore
              .findFirstSequencerStateForSequencer(sequencerId)
              .map(txOpt =>
                txOpt
                  .map(stored => stored.validFrom)
                  .toRight(s"Did not find onboarding topology transaction for $sequencerId")
              )
          )
        case Right(timestamp) =>
          EitherT.rightT[Future, String](EffectiveTime(timestamp))
      }

      _ <- domainTimeTracker
        .awaitTick(referenceEffective.value)
        .map(EitherT.right[String](_).void)
        .getOrElse(EitherTUtil.unit[String])

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

      topologySnapshot <- EitherT.right[String](
        topologyStore.findEssentialStateAtSequencedTime(
          SequencedTime(sequencerSnapshot.lastTs),
          excludeMappings = Nil,
        )
      )
    } yield (topologySnapshot, sequencerSnapshot))
      .fold[v30.OnboardingStateResponse](
        error =>
          v30.OnboardingStateResponse(
            v30.OnboardingStateResponse.Value.Failure(
              v30.OnboardingStateResponse.Failure(error)
            )
          ),
        { case (topologySnapshot, sequencerSnapshot) =>
          v30.OnboardingStateResponse(
            v30.OnboardingStateResponse.Value.Success(
              v30.OnboardingStateResponse.Success(
                OnboardingStateForSequencer(
                  topologySnapshot,
                  staticDomainParameters,
                  sequencerSnapshot,
                  staticDomainParameters.protocolVersion,
                ).toByteString
              )
            )
          )
        },
      )
  }

  override def genesisState(request: v30.GenesisStateRequest): Future[v30.GenesisStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result = for {
      timestampO <- EitherT
        .fromEither[Future](
          request.timestamp.traverse(CantonTimestamp.fromProtoTimestamp)
        )
        .leftMap(_.toString)

      sequencedTimestamp <- timestampO match {
        case Some(value) => EitherT.rightT[Future, String](value)
        case None =>
          val sequencedTimeF = topologyStore
            .maxTimestamp()
            .collect {
              case Some((sequencedTime, _)) =>
                Right(sequencedTime.value)

              case None => Left("No sequenced time found")
            }

          EitherT(sequencedTimeF)
      }

      topologySnapshot <- EitherT.right[String](
        topologyStore.findEssentialStateAtSequencedTime(
          SequencedTime(sequencedTimestamp),
          // we exclude vetted packages from the genesis state because we need to upload them again anyway
          excludeMappings = Seq(TopologyMappingX.Code.VettedPackagesX),
        )
      )
      // reset effective time and sequenced time if we are initializing the sequencer from the beginning
      genesisState: StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX] =
        StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX](
          topologySnapshot.result.map(stored =>
            StoredTopologyTransactionX(
              SequencedTime(SignedTopologyTransactionX.InitialTopologySequencingTime),
              EffectiveTime(SignedTopologyTransactionX.InitialTopologySequencingTime),
              stored.validUntil.map(_ =>
                EffectiveTime(SignedTopologyTransactionX.InitialTopologySequencingTime)
              ),
              stored.transaction,
            )
          )
        )

    } yield genesisState.toByteString(staticDomainParameters.protocolVersion)

    result
      .fold[v30.GenesisStateResponse](
        error =>
          v30.GenesisStateResponse(
            v30.GenesisStateResponse.Value.Failure(v30.GenesisStateResponse.Failure(error))
          ),
        result =>
          v30.GenesisStateResponse(
            v30.GenesisStateResponse.Value.Success(
              v30.GenesisStateResponse.Success(result)
            )
          ),
      )
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
        serial <- wrapErrUS(ProtoConverter.parsePositiveInt(requestP.serial))
        totalTrafficBalance <- wrapErrUS(
          ProtoConverter.parseNonNegativeLong(requestP.totalTrafficBalance)
        )
        highestMaxSequencingTimestamp <- sequencer
          .setTrafficBalance(member, serial, totalTrafficBalance, sequencerClient)
          .leftWiden[CantonError]
      } yield SetTrafficBalanceResponse(
        maxSequencingTimestamp = Some(highestMaxSequencingTimestamp.toProtoTimestamp)
      )
    }

    mapErrNewEUS(result)
  }
}
