// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  InvariantViolation,
  ProtoDeserializationFailure,
  ValueDeserializationError,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.protocol.{StaticSynchronizerParameters, v30}
import com.digitalasset.canton.sequencer.admin.v30.SequencerInitializationServiceGrpc.SequencerInitializationService
import com.digitalasset.canton.sequencer.admin.v30.{
  InitializeSequencerFromGenesisStateRequest,
  InitializeSequencerFromGenesisStateResponse,
  InitializeSequencerFromGenesisStateV2Request,
  InitializeSequencerFromGenesisStateV2Response,
  InitializeSequencerFromOnboardingStateRequest,
  InitializeSequencerFromOnboardingStateResponse,
  InitializeSequencerFromOnboardingStateV2Request,
  InitializeSequencerFromOnboardingStateV2Response,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.synchronizer.Synchronizer.FailedToInitialiseSynchronizerNode
import com.digitalasset.canton.synchronizer.sequencer.admin.grpc.{
  InitializeSequencerRequest,
  InitializeSequencerResponse,
}
import com.digitalasset.canton.synchronizer.sequencer.{
  OnboardingStateForSequencer,
  OnboardingStateForSequencerV2,
  SequencerSnapshot,
}
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
}
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils}
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerInitializationService(
    handler: GrpcSequencerInitializationService.Callback,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends SequencerInitializationService
    with NamedLogging {

  override def initializeSequencerFromGenesisState(
      responseObserver: StreamObserver[InitializeSequencerFromGenesisStateResponse]
  ): StreamObserver[InitializeSequencerFromGenesisStateRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    GrpcStreamingUtils.streamFromClient(
      _.topologySnapshot,
      _.synchronizerParameters,
      (
          topologySnapshot: ByteString,
          synchronizerParams: Option[v30.StaticSynchronizerParameters],
      ) => initializeSequencerFromGenesisState(topologySnapshot, synchronizerParams),
      responseObserver,
    )
  }

  private def initializeSequencerFromGenesisState(
      topologySnapshot: ByteString,
      synchronizerParameters: Option[v30.StaticSynchronizerParameters],
  )(implicit traceContext: TraceContext): Future[InitializeSequencerFromGenesisStateResponse] = {
    val res: EitherT[Future, RpcError, InitializeSequencerFromGenesisStateResponse] = for {
      topologyState <- EitherT.fromEither[Future](
        StoredTopologyTransactions
          .fromTrustedByteString(topologySnapshot)
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      replicated <- initializeSequencerFromGenesisStateInternal(
        topologyState,
        synchronizerParameters,
      )
    } yield InitializeSequencerFromGenesisStateResponse(replicated)
    mapErrNew(res)
  }

  override def initializeSequencerFromGenesisStateV2(
      responseObserver: StreamObserver[InitializeSequencerFromGenesisStateV2Response]
  ): StreamObserver[InitializeSequencerFromGenesisStateV2Request] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    GrpcStreamingUtils.streamFromClient(
      _.topologySnapshot,
      _.synchronizerParameters,
      (
          topologySnapshot: ByteString,
          synchronizerParams: Option[v30.StaticSynchronizerParameters],
      ) => initializeSequencerFromGenesisStateV2(topologySnapshot, synchronizerParams),
      responseObserver,
    )
  }

  private def initializeSequencerFromGenesisStateV2(
      topologySnapshot: ByteString,
      synchronizerParameters: Option[v30.StaticSynchronizerParameters],
  )(implicit
      traceContext: TraceContext
  ): Future[InitializeSequencerFromGenesisStateV2Response] = {
    val res: EitherT[Future, RpcError, InitializeSequencerFromGenesisStateV2Response] = for {
      topologyState <- EitherT.fromEither[Future](
        GrpcStreamingUtils
          .parseDelimitedFromTrusted(
            topologySnapshot.newInput(),
            StoredTopologyTransaction,
          )
          .bimap(
            msg =>
              ProtoDeserializationFailure.Wrap(ValueDeserializationError("topology_snapshot", msg)),
            StoredTopologyTransactions(_),
          )
      )
      replicated <- initializeSequencerFromGenesisStateInternal(
        topologyState,
        synchronizerParameters,
      )
    } yield InitializeSequencerFromGenesisStateV2Response(replicated)
    mapErrNew(res)
  }

  private def initializeSequencerFromGenesisStateInternal(
      topologyState: GenericStoredTopologyTransactions,
      synchronizerParameters: Option[v30.StaticSynchronizerParameters],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, RpcError, Boolean] =
    for {
      synchronizerParameters <- EitherT.fromEither[Future](
        ProtoConverter
          .parseRequired(
            StaticSynchronizerParameters.fromProtoV30,
            "synchronizer_parameters",
            synchronizerParameters,
          )
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      // reset effective time and sequenced time if we are initializing the sequencer from the beginning
      genesisState: StoredTopologyTransactions[TopologyChangeOp, TopologyMapping] =
        StoredTopologyTransactions[TopologyChangeOp, TopologyMapping](
          topologyState.result.map(stored =>
            StoredTopologyTransaction(
              SequencedTime(SignedTopologyTransaction.InitialTopologySequencingTime),
              EffectiveTime(SignedTopologyTransaction.InitialTopologySequencingTime),
              stored.validUntil.map(_ =>
                EffectiveTime(SignedTopologyTransaction.InitialTopologySequencingTime)
              ),
              stored.transaction,
              stored.rejectionReason,
            )
          )
        )

      // check that the snapshot is consistent with respect to effective proposals and effective fully authorized transactions
      multipleEffectivePerUniqueKey = genesisState.result
        .groupBy(_.transaction.mapping.uniqueKey)
        // only retain effective transactions
        .values
        .toSeq
        .flatMap { topoTxs =>
          val (proposals, fullyAuthorized) =
            topoTxs.filter(tx => tx.validUntil.isEmpty).partition(_.transaction.isProposal)

          val maxFullyAuthorizedSerialO = fullyAuthorized.view.map(_.serial).maxOption

          val allProposalsLessThanOrEqualMaxSerial = {
            val unexpectedEffectiveProposals = maxFullyAuthorizedSerialO.toList.flatMap {
              maxFullyAuthorizedSerial => proposals.filter(_.serial <= maxFullyAuthorizedSerial)
            }
            if (unexpectedEffectiveProposals.nonEmpty) {
              Seq(
                "effective proposals with serial less than or equal to the highest fully authorized transaction" -> unexpectedEffectiveProposals
              )
            } else Seq.empty
          }

          val multipleEffectiveProposalsWithDifferentSerials =
            if (proposals.map(_.serial).distinct.sizeIs > 1) {
              Seq("muliple effective proposals with different serials" -> proposals)
            } else Seq.empty

          val multipleEffectiveProposalsForSameTransactionHash =
            proposals
              .groupBy(_.hash)
              .filter(_._2.sizeIs > 1)
              .values
              .map(
                "multiple effective proposals for the same transaction hash" -> _
              )

          val multipleFullyAuthorized = if (fullyAuthorized.sizeIs > 1) {
            Seq("concurrently effective transactions with the same unique key" -> fullyAuthorized)
          } else Seq.empty

          multipleFullyAuthorized ++ multipleEffectiveProposalsForSameTransactionHash ++ allProposalsLessThanOrEqualMaxSerial ++ multipleEffectiveProposalsWithDifferentSerials
        }

      _ <- EitherTUtil.condUnitET[Future](
        multipleEffectivePerUniqueKey.isEmpty,
        TopologyManagerError.InconsistentTopologySnapshot.MultipleEffectiveMappingsPerUniqueKey(
          multipleEffectivePerUniqueKey
        ),
      )

      initializeRequest = InitializeSequencerRequest(genesisState, synchronizerParameters, None)
      result <- handler
        .initialize(initializeRequest)
        .leftMap(FailedToInitialiseSynchronizerNode.Failure(_))
        .onShutdown(Left(FailedToInitialiseSynchronizerNode.Shutdown())): EitherT[
        Future,
        RpcError,
        InitializeSequencerResponse,
      ]
    } yield result.replicated

  override def initializeSequencerFromOnboardingState(
      responseObserver: StreamObserver[InitializeSequencerFromOnboardingStateResponse]
  ): StreamObserver[InitializeSequencerFromOnboardingStateRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    GrpcStreamingUtils.streamFromClient(
      _.onboardingState,
      _ => (),
      (onboardingState: ByteString, _: Unit) =>
        initializeSequencerFromOnboardingState(onboardingState),
      responseObserver,
    )
  }

  private def initializeSequencerFromOnboardingState(
      onboardingState: ByteString
  )(implicit traceContext: TraceContext) = {
    val res = for {
      onboardingState <- EitherT.fromEither[Future](
        OnboardingStateForSequencer
          // according to @rgugliel-da, this is safe to do here.
          // the caller of this endpoint could get the onboarding state from various sequencers
          // and compare them for byte-for-byte equality, to increase the confidence that this
          // is safe to deserialize
          .fromTrustedByteString(onboardingState)
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      replicated <- initializeSequencerFromOnboardingStateInternal(
        onboardingState.topologySnapshot,
        onboardingState.staticSynchronizerParameters,
        onboardingState.sequencerSnapshot,
      )

    } yield InitializeSequencerFromOnboardingStateResponse(replicated)
    mapErrNew(res)
  }

  override def initializeSequencerFromOnboardingStateV2(
      responseObserver: StreamObserver[InitializeSequencerFromOnboardingStateV2Response]
  ): StreamObserver[InitializeSequencerFromOnboardingStateV2Request] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    GrpcStreamingUtils.streamFromClient(
      _.onboardingState,
      _ => (),
      (onboardingState: ByteString, _: Unit) =>
        initializeSequencerFromOnboardingStateV2(onboardingState),
      responseObserver,
    )
  }

  private def initializeSequencerFromOnboardingStateV2(
      onboardingStateBytes: ByteString
  )(implicit
      traceContext: TraceContext
  ): Future[InitializeSequencerFromOnboardingStateV2Response] = {
    val in = onboardingStateBytes.newInput()
    val res = for {
      onboardState <- EitherT.fromEither[Future](
        GrpcStreamingUtils
          .parseDelimitedFromTrusted(in, OnboardingStateForSequencerV2)
          .leftMap(err =>
            ProtoDeserializationFailure.Wrap(
              ProtoDeserializationError.ValueConversionError("onboarding_state", err)
            )
          )
      )
      accumulatedOnboardingState <- EitherT
        .fromEither[Future](
          onboardState.foldM[
            Either[ProtoDeserializationError, *],
            (
                Vector[GenericStoredTopologyTransaction],
                Option[StaticSynchronizerParameters],
                Option[SequencerSnapshot],
            ),
          ](
            (
              Vector.empty[GenericStoredTopologyTransaction],
              Option.empty[StaticSynchronizerParameters],
              Option.empty[SequencerSnapshot],
            )
          ) {
            case (
                  (prevTxs, prevStaticParams, prevSeqSnapshot),
                  OnboardingStateForSequencerV2(newTx, newStaticParams, newSeqSnapshot),
                ) =>
              for {
                static <- checkForEqual(
                  "static_synchronizer_parameters",
                  prevStaticParams,
                  newStaticParams,
                )
                seqSnapshot <- checkForEqual("sequencer_snapshot", prevSeqSnapshot, newSeqSnapshot)
              } yield (prevTxs ++ newTx, static, seqSnapshot)
          }
        )
        .leftMap(ProtoDeserializationFailure.Wrap(_))

      (allTxs, staticParamsO, seqSnapshotO) = accumulatedOnboardingState

      staticParams <- EitherT.fromEither[Future](
        staticParamsO.toRight[RpcError](
          ProtoDeserializationFailure.Wrap(FieldNotSet("static_synchronizer_parameters"))
        )
      )
      seqSnapshot <- EitherT.fromEither[Future](
        seqSnapshotO.toRight[RpcError](
          ProtoDeserializationFailure.Wrap(FieldNotSet("sequencer_snapshot"))
        )
      )
      replicated <- initializeSequencerFromOnboardingStateInternal(
        StoredTopologyTransactions(allTxs),
        staticParams,
        seqSnapshot,
      )
    } yield InitializeSequencerFromOnboardingStateV2Response(replicated)
    mapErrNew(res)
  }

  private def checkForEqual[A](
      field: String,
      x: Option[A],
      y: Option[A],
  ): Either[ProtoDeserializationError, Option[A]] = (x, y) match {
    case (None, None) => Right(None)
    case (Some(a), Some(b)) =>
      Either.cond(
        a == b,
        Some(b),
        InvariantViolation(field, s"Multiple provided values didn't match: a=[$a], b=[$b]"),
      )
    case (a, b) => Right(a.orElse(b))
  }

  private def initializeSequencerFromOnboardingStateInternal(
      topologySnapshot: GenericStoredTopologyTransactions,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      sequencerSnapshot: SequencerSnapshot,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, RpcError, Boolean] = {
    val initializeRequest = InitializeSequencerRequest(
      topologySnapshot,
      staticSynchronizerParameters,
      Some(sequencerSnapshot),
    )
    handler
      .initialize(initializeRequest)
      .bimap(
        FailedToInitialiseSynchronizerNode.Failure(_),
        result => result.replicated,
      )
      .onShutdown(Left(FailedToInitialiseSynchronizerNode.Shutdown()))
  }
}

object GrpcSequencerInitializationService {
  trait Callback {
    def initialize(request: InitializeSequencerRequest)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, InitializeSequencerResponse]
  }
}
