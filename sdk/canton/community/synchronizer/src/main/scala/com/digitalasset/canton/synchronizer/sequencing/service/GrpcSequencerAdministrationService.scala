// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencer.admin.v30.{
  GetLSUTrafficControlStateRequest,
  GetLSUTrafficControlStateResponse,
  OnboardingStateResponse,
  OnboardingStateV2Request,
  OnboardingStateV2Response,
  SetLSUTrafficControlStateRequest,
  SetLSUTrafficControlStateResponse,
  SetTrafficPurchasedRequest,
  SetTrafficPurchasedResponse,
}
import com.digitalasset.canton.sequencing.client.SequencerClientSend
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.synchronizer.sequencer.traffic.{LSUTrafficState, TimestampSelector}
import com.digitalasset.canton.synchronizer.sequencer.{
  OnboardingStateForSequencer,
  OnboardingStateForSequencerV2,
  Sequencer,
  SequencerSnapshot,
}
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{StoredTopologyTransactions, TopologyStore}
import com.digitalasset.canton.topology.transaction.SequencerSynchronizerState
import com.digitalasset.canton.topology.{
  Member,
  SequencerId,
  TopologyManagerError,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils}
import com.digitalasset.canton.version.ProtoVersion
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import java.io.OutputStream
import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerAdministrationService(
    sequencer: Sequencer,
    sequencerClient: SequencerClientSend,
    topologyStore: TopologyStore[SynchronizerStore],
    topologyClient: SynchronizerTopologyClient,
    synchronizerTimeTracker: SynchronizerTimeTracker,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext,
    materializer: Materializer,
) extends v30.SequencerAdministrationServiceGrpc.SequencerAdministrationService
    with NamedLogging {

  override def pruningStatus(
      request: v30.PruningStatusRequest
  ): Future[v30.PruningStatusResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    sequencer.pruningStatus
      .map(_.toProtoV30)
      .map(status => v30.PruningStatusResponse(Some(status)))
      .asGrpcResponse
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
    val result = (for {
      timestamp <- wrapErrUS(
        ProtoConverter
          .parseRequired(CantonTimestamp.fromProtoTimestamp, "timestamp", request.timestamp)
      )
      result <- sequencer.snapshot(timestamp).leftMap(_.toCantonRpcError)
    } yield result)
      .fold[v30.SnapshotResponse](
        error =>
          v30.SnapshotResponse(
            v30.SnapshotResponse.Value.Failure(v30.SnapshotResponse.Failure(error.cause))
          ),
        result =>
          v30.SnapshotResponse(
            v30.SnapshotResponse.Value.VersionedSuccess(
              v30.SnapshotResponse.VersionedSuccess(result.toByteString)
            )
          ),
      )

    shutdownAsGrpcError(result)
  }

  override def onboardingState(
      request: v30.OnboardingStateRequest,
      responseObserver: StreamObserver[OnboardingStateResponse],
  ): Unit =
    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => {
        implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
        val res =
          for {
            memberOrTimestamp <- memberOrTimestamp(
              request.request.sequencerUid,
              request.request.timestamp,
            )
            seqSnapshotAndSource <- onboardingStateSource(memberOrTimestamp)
            (sequencerSnapshot, snapshotSource) = seqSnapshotAndSource
            storedTransactions <- EitherT
              .right[RpcError](snapshotSource.runWith(Sink.seq))
              .mapK(FutureUnlessShutdown.outcomeK)
          } yield {
            val onboardingState = OnboardingStateForSequencer(
              StoredTopologyTransactions(storedTransactions),
              staticSynchronizerParameters,
              sequencerSnapshot,
            )
            onboardingState.toByteString.writeTo(out)
          }
        mapErrNewEUS(res)
      },
      responseObserver,
      byteString => OnboardingStateResponse(byteString),
    )

  override def onboardingStateV2(
      request: OnboardingStateV2Request,
      responseObserver: StreamObserver[OnboardingStateV2Response],
  ): Unit =
    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => {
        implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
        val res = for {
          memberOrTimestamp <- memberOrTimestamp(
            request.request.sequencerUid,
            request.request.timestamp,
          )
          seqSnapshotAndSource <- onboardingStateSource(memberOrTimestamp)
          (sequencerSnapshot, snapshotSource) = seqSnapshotAndSource

          nonTopologyOnboardingState = OnboardingStateForSequencerV2(
            None,
            Some(staticSynchronizerParameters),
            Some(sequencerSnapshot),
            staticSynchronizerParameters.protocolVersion,
          )
          _ = nonTopologyOnboardingState.writeDelimitedTo(out)
          _ <- EitherT
            .right[RpcError](
              snapshotSource.runWith(
                Sink.foreachAsync(1)(stored =>
                  mapErrNewEUS(
                    wrapErrUS(
                      OnboardingStateForSequencerV2(
                        Some(stored),
                        None,
                        None,
                        staticSynchronizerParameters.protocolVersion,
                      )
                        .writeDelimitedTo(out)
                        .leftMap(
                          ProtoDeserializationError.ValueConversionError("onboarding_state", _)
                        )
                    )
                  )
                )
              )
            )
            .mapK(FutureUnlessShutdown.outcomeK)
        } yield ()
        mapErrNewEUS(res)
      },
      responseObserver,
      byteString => OnboardingStateV2Response(byteString),
    )

  private def memberOrTimestamp(memberP: Option[String], timestampP: Option[Timestamp])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Either[SequencerId, CantonTimestamp]] = {
    val sequencerId = memberP.map(
      UniqueIdentifier.fromProtoPrimitive(_, "sequencer_uid").map(uid => Left(SequencerId(uid)))
    )
    val timestamp = timestampP.map(CantonTimestamp.fromProtoTimestamp(_).map(Right(_)))

    val resultE = sequencerId.orElse(timestamp).getOrElse(Left(FieldNotSet("sequencer_uid")))
    wrapErrUS(resultE)
  }

  private def onboardingStateSource(
      memberOrTimestamp: Either[SequencerId, CantonTimestamp]
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    RpcError,
    (SequencerSnapshot, Source[GenericStoredTopologyTransaction, NotUsed]),
  ] =
    for {
      referenceEffective <- memberOrTimestamp match {
        case Left(sequencerId) =>
          EitherT(
            topologyStore
              .findFirstSequencerStateForSequencer(sequencerId)
              .map(txOpt =>
                txOpt
                  .map(stored => stored.validFrom)
                  .toRight(
                    TopologyManagerError.MissingTopologyMapping
                      .Reject(Map(sequencerId -> Seq(SequencerSynchronizerState.code)))
                      .toCantonRpcError
                  )
              )
          )
        case Right(timestamp) =>
          EitherT.rightT[FutureUnlessShutdown, RpcError](EffectiveTime(timestamp))
      }

      /* wait for the sequencer snapshot that contains a sequenced timestamp that is >= the reference/onboarding effective time
       if we take the sequencing time here, we might miss out topology transactions between effectiveTime
       and block.lastTs of the block containing the effectiveTime in the following scenario:
        t0: onboarding sequenced time
        t1: sequenced time of some topology transaction
        t2: onboarding effective time
        t3: lastTs of the block containing t2

        Therefore, if we find the lastTs of a block that "contains" the onboarding effective time,
        and we then use it as the reference sequenced time for fetching the topology snapshot,
        we can be sure that
        a) the topology snapshot contains all topology transactions sequenced up to including the onboarding effective time
        b) the topology snapshot might contain a few more transactions between the onboarding effective time and the last sequenced time in the block
        c) the sequencer snapshot will contain the correct counter for the onboarding sequencer
        d) the onboarding sequencer will properly subscribe from its own minimum counter that it gets initialized with from the sequencer snapshot
       */

      // Ensure there is a block containing the reference effective time
      _ <- synchronizerTimeTracker
        .awaitTick(referenceEffective.value)
        .traverse_(EitherTUtil.rightUS[RpcError, Unit](_))

      sequencerSnapshotTimestamp <- sequencer
        .awaitContainingBlockLastTimestamp(referenceEffective.value)
        .leftMap(_.toCantonRpcError)

      // Wait for the domain time tracker to observe the sequencerSnapshot.lastTs.
      // This is only serves as a potential trigger for the topology client, in case no
      // additional message comes in, because topologyClient.awaitSequencedTimestamp does not
      // trigger a tick.
      _ <- synchronizerTimeTracker
        .awaitTick(sequencerSnapshotTimestamp)
        .traverse_(EitherTUtil.rightUS[RpcError, Unit](_))

      // wait for the sequencer snapshot's lastTs to be observed by the topology client,
      // which implies that all topology transactions with a sequenced time up to including the
      // sequencer snapshot's lastTs will have been properly processed and stored (albeit maybe not yet effective,
      // but that's not relevent for the purpose of the topology snapshot to export).
      _ <- EitherT
        .right[RpcError](
          topologyClient
            .awaitSequencedTimestamp(SequencedTime(sequencerSnapshotTimestamp))
            .getOrElse(FutureUnlessShutdown.unit)
        )

      sequencerSnapshot <- sequencer
        .awaitSnapshot(sequencerSnapshotTimestamp)
        .leftMap(_.toCantonRpcError)

    } yield {
      sequencerSnapshot -> topologyStore
        .findEssentialStateAtSequencedTime(
          asOfInclusive = SequencedTime(sequencerSnapshot.lastTs),
          // we need to include the rejected transactions as well, because they might have an impact on the TopologyTimestampPlusEpsilonTracker
          includeRejected = true,
        )
    }

  override def disableMember(
      requestP: v30.DisableMemberRequest
  ): Future[v30.DisableMemberResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result = for {
      member <- EitherT.fromEither[FutureUnlessShutdown](
        Member
          .fromProtoPrimitive(requestP.member, "member")
          .leftMap(err =>
            new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(err.toString))
          )
      )
      _ <- sequencer.disableMember(member).leftMap(_.asGrpcError)
    } yield v30.DisableMemberResponse()

    result.asGrpcResponse
  }

  /** Update the traffic purchased entry of a member The top up will only become valid once
    * authorized by all sequencers of the synchronizer
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
          synchronizerTimeTracker,
        )
        .leftWiden[RpcError]
    } yield SetTrafficPurchasedResponse()

    mapErrNewEUS(result)
  }

  /** Get the traffic control state at the Logical Synchronizer Upgrade time. Only available once
    * sequencer node has reached the upgrade time, otherwise returns an empty map.
    */
  override def getLSUTrafficControlState(
      request: GetLSUTrafficControlStateRequest
  ): Future[GetLSUTrafficControlStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result = sequencer.getLSUTrafficControlState
      .map(trafficState => GetLSUTrafficControlStateResponse(trafficState.toByteString))
      .leftMap(_.toCantonRpcError)
    mapErrNewEUS(result)
  }

  /** Set the traffic control state at the Logical Synchronizer Upgrade time. Can only to be used
    * during the upgrade process, can be called successfully once, only works if sequencer node
    * hasn't progressed beyond the upgrade time, otherwise returns an error.
    */
  override def setLSUTrafficControlState(
      request: SetLSUTrafficControlStateRequest
  ): Future[SetLSUTrafficControlStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {
      protocolVersionForProtoV30 <- wrapErrUS(
        LSUTrafficState.protocolVersionRepresentativeFor(ProtoVersion(30))
      )
      trafficStates <- wrapErrUS(
        LSUTrafficState.fromByteString(
          protocolVersionForProtoV30.representative,
          request.lsuTrafficState,
        )
      )
      _ <-
        sequencer
          .setLSUTrafficControlState(trafficStates)
          .leftMap(_.toCantonRpcError)
    } yield SetLSUTrafficControlStateResponse()

    mapErrNewEUS(result)
  }
}
