// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrderer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.{
  AvailabilityModule,
  AvailabilityModuleConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  EpochStoreReader,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.{
  PreIssConsensusModule,
  SegmentModuleRefFactoryImpl,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.mempool.{
  MempoolModule,
  MempoolModuleConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.RequestInspector
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.{
  BftP2PNetworkIn,
  BftP2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.OrderingTopologyProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.SystemInitializer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.OrderingModuleSystemInitializer.ModuleFactories
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Mempool
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.{
  AvailabilityModuleDependencies,
  ConsensusModuleDependencies,
  P2PNetworkOutModuleDependencies,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  BlockSubscription,
  Env,
  OrderingModuleSystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveRequest
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.version.ProtocolVersion

import scala.util.Random

object BftOrderingModuleSystemInitializer {

  /** A module system initializer for the concrete Canton BFT ordering system.
    */
  private[bftordering] def apply[E <: Env[E]](
      protocolVersion: ProtocolVersion,
      bootstrapTopologyInfo: OrderingTopologyInfo[E],
      config: BftBlockOrderer.Config,
      initialApplicationHeight: BlockNumber,
      epochLength: EpochLength,
      stores: BftOrderingStores[E],
      orderingTopologyProvider: OrderingTopologyProvider[E],
      blockSubscription: BlockSubscription,
      sequencerSnapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo],
      clock: Clock,
      random: Random,
      metrics: BftOrderingMetrics,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      requestInspector: RequestInspector =
        OutputModule.DefaultRequestInspector, // Only set by simulation tests
  )(implicit
      mc: MetricsContext
  ): SystemInitializer[E, BftOrderingServiceReceiveRequest, Mempool.Message] = {
    val thisPeerFirstKnownAt =
      sequencerSnapshotAdditionalInfo.flatMap(_.peerActiveAt.get(bootstrapTopologyInfo.thisPeer))
    val firstBlockNumberInOnboardingEpoch = thisPeerFirstKnownAt.flatMap(_.firstBlockNumberInEpoch)
    val previousBftTimeForOnboarding = thisPeerFirstKnownAt.flatMap(_.previousBftTime)
    val onboardingEpochCouldAlterOrderingTopology =
      thisPeerFirstKnownAt
        .flatMap(_.epochCouldAlterOrderingTopology)
        .exists(pendingChanges => pendingChanges)
    val outputModuleStartupState =
      OutputModule.StartupState(
        // Note that the initial height for the block subscription below might be different (when onboarding after genesis).
        initialHeight = firstBlockNumberInOnboardingEpoch.getOrElse(initialApplicationHeight),
        previousBftTimeForOnboarding,
        onboardingEpochCouldAlterOrderingTopology,
        bootstrapTopologyInfo.currentCryptoProvider,
        bootstrapTopologyInfo.currentTopology,
      )
    OrderingModuleSystemInitializer(
      ModuleFactories(
        mempool = { availabilityRef =>
          val cfg = MempoolModuleConfig(
            config.maxMempoolQueueSize,
            config.maxRequestPayloadBytes,
            config.maxRequestsInBatch,
            config.minRequestsInBatch,
            config.maxBatchCreationInterval,
          )
          new MempoolModule(
            cfg,
            metrics,
            availabilityRef,
            loggerFactory,
            timeouts,
          )
        },
        p2pNetworkIn = (availabilityRef, consensusRef) =>
          new BftP2PNetworkIn(
            metrics,
            availabilityRef,
            consensusRef,
            loggerFactory,
            timeouts,
          ),
        p2pNetworkOut =
          (networkManager, p2pNetworkInRef, mempoolRef, availabilityRef, consensusRef, outputRef) =>
            {
              val dependencies = P2PNetworkOutModuleDependencies(
                networkManager,
                p2pNetworkInRef,
                mempoolRef,
                availabilityRef,
                consensusRef,
                outputRef,
              )
              new BftP2PNetworkOut(
                bootstrapTopologyInfo.thisPeer,
                stores.p2pEndpointsStore,
                metrics,
                dependencies,
                loggerFactory,
                timeouts,
              )
            },
        availability = (mempoolRef, networkOutRef, consensusRef, outputRef) => {
          val cfg = AvailabilityModuleConfig(
            config.maxBatchesPerBlockProposal,
            config.outputFetchTimeout,
          )
          val dependencies = AvailabilityModuleDependencies[E](
            mempoolRef,
            networkOutRef,
            consensusRef,
            outputRef,
          )
          new AvailabilityModule[E](
            bootstrapTopologyInfo.currentMembership,
            bootstrapTopologyInfo.currentCryptoProvider,
            stores.availabilityStore,
            cfg,
            clock,
            random,
            metrics,
            dependencies,
            loggerFactory,
            timeouts,
          )
        },
        consensus = (p2pNetworkOutRef, availabilityRef, outputRef) => {
          val dependencies = ConsensusModuleDependencies(
            availabilityRef,
            outputRef,
            p2pNetworkOutRef,
          )

          val segmentModuleRefFactory = new SegmentModuleRefFactoryImpl(
            storePbftMessages = true,
            stores.epochStore,
            dependencies,
            clock,
            loggerFactory,
            timeouts,
          )

          new PreIssConsensusModule(
            bootstrapTopologyInfo,
            epochLength,
            stores.epochStore,
            sequencerSnapshotAdditionalInfo,
            clock,
            metrics,
            segmentModuleRefFactory,
            dependencies,
            loggerFactory,
            timeouts,
          )
        },
        output = (availabilityRef, consensusRef) =>
          new OutputModule(
            outputModuleStartupState,
            orderingTopologyProvider,
            stores.outputStore,
            stores.epochStoreReader,
            blockSubscription,
            metrics,
            protocolVersion,
            availabilityRef,
            consensusRef,
            loggerFactory,
            timeouts,
            requestInspector,
          ),
      )
    )
  }

  final case class BftOrderingStores[E <: Env[E]](
      p2pEndpointsStore: P2PEndpointsStore[E],
      availabilityStore: AvailabilityStore[E],
      epochStore: EpochStore[E],
      epochStoreReader: EpochStoreReader[E],
      outputStore: OutputMetadataStore[E],
  )
}
