// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.protocol.{StaticSynchronizerParameters, v30}
import com.digitalasset.canton.sequencer.admin.v30.SequencerInitializationServiceGrpc.SequencerInitializationService
import com.digitalasset.canton.sequencer.admin.v30.{
  InitializeSequencerFromGenesisStateRequest,
  InitializeSequencerFromGenesisStateResponse,
  InitializeSequencerFromOnboardingStateRequest,
  InitializeSequencerFromOnboardingStateResponse,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.synchronizer.Synchronizer.FailedToInitialiseSynchronizerNode
import com.digitalasset.canton.synchronizer.sequencer.OnboardingStateForSequencer
import com.digitalasset.canton.synchronizer.sequencer.admin.grpc.{
  InitializeSequencerRequest,
  InitializeSequencerResponse,
}
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
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
  ): StreamObserver[InitializeSequencerFromGenesisStateRequest] =
    GrpcStreamingUtils.streamFromClient(
      _.topologySnapshot,
      _.synchronizerParameters,
      (
          topologySnapshot: ByteString,
          synchronizerParams: Option[v30.StaticSynchronizerParameters],
      ) => initializeSequencerFromGenesisState(topologySnapshot, synchronizerParams),
      responseObserver,
    )

  private def initializeSequencerFromGenesisState(
      topologySnapshot: ByteString,
      synchronizerParameters: Option[v30.StaticSynchronizerParameters],
  ): Future[InitializeSequencerFromGenesisStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res: EitherT[Future, RpcError, InitializeSequencerFromGenesisStateResponse] = for {
      topologyState <- EitherT.fromEither[Future](
        StoredTopologyTransactions
          .fromTrustedByteString(topologySnapshot)
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )

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
    } yield InitializeSequencerFromGenesisStateResponse(result.replicated)
    mapErrNew(res)
  }

  override def initializeSequencerFromOnboardingState(
      responseObserver: StreamObserver[InitializeSequencerFromOnboardingStateResponse]
  ): StreamObserver[InitializeSequencerFromOnboardingStateRequest] =
    GrpcStreamingUtils.streamFromClient(
      _.onboardingState,
      _ => (),
      (onboardingState: ByteString, _: Unit) =>
        initializeSequencerFromOnboardingState(onboardingState),
      responseObserver,
    )

  private def initializeSequencerFromOnboardingState(onboardingState: ByteString) = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res: EitherT[Future, RpcError, InitializeSequencerFromOnboardingStateResponse] = for {
      onboardingState <- EitherT.fromEither[Future](
        OnboardingStateForSequencer
          // according to @rgugliel-da, this is safe to do here.
          // the caller of this endpoint could get the onboarding state from various sequencers
          // and compare them for byte-for-byte equality, to increase the confidence that this
          // is safe to deserialize
          .fromTrustedByteString(onboardingState)
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      initializeRequest = InitializeSequencerRequest(
        onboardingState.topologySnapshot,
        onboardingState.staticSynchronizerParameters,
        Some(onboardingState.sequencerSnapshot),
      )
      result <- handler
        .initialize(initializeRequest)
        .leftMap(FailedToInitialiseSynchronizerNode.Failure(_))
        .onShutdown(Left(FailedToInitialiseSynchronizerNode.Shutdown())): EitherT[
        Future,
        RpcError,
        InitializeSequencerResponse,
      ]
    } yield InitializeSequencerFromOnboardingStateResponse(result.replicated)
    mapErrNew(res)
  }
}

object GrpcSequencerInitializationService {
  trait Callback {
    def initialize(request: InitializeSequencerRequest)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, InitializeSequencerResponse]
  }
}
