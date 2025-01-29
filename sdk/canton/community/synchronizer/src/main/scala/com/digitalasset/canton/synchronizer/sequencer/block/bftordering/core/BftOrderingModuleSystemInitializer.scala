// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrderer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.P2pEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.{
  BftP2PNetworkIn,
  BftP2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  OrderingTopologyProvider,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.SystemInitializer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.OrderingModuleSystemInitializer.ModuleFactories
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
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
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1.BftOrderingServiceReceiveRequest
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.version.ProtocolVersion

import scala.util.Random

import OutputModule.RequestInspector
import modules.availability.{AvailabilityModule, AvailabilityModuleConfig}
import modules.consensus.iss.data.{EpochStore, OrderedBlocksReader}
import modules.consensus.iss.{PreIssConsensusModule, SegmentModuleRefFactoryImpl}
import modules.mempool.{MempoolModule, MempoolModuleConfig}

object BftOrderingModuleSystemInitializer {

  /** A module system initializer for the concrete Canton BFT ordering system.
    */
  private[bftordering] def apply[E <: Env[E]](
      thisPeer: SequencerId,
      protocolVersion: ProtocolVersion,
      initialOrderingTopology: OrderingTopology,
      initialCryptoProvider: CryptoProvider[E],
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
    val initialMembership = Membership(thisPeer, initialOrderingTopology)
    val thisPeerFirstKnownAt =
      sequencerSnapshotAdditionalInfo.flatMap(_.peerActiveAt.get(thisPeer))
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
        initialCryptoProvider,
        initialOrderingTopology,
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
                thisPeer,
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
            initialMembership,
            initialCryptoProvider,
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
            initialMembership,
            initialCryptoProvider,
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
            stores.orderedBlocksReader,
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
      p2pEndpointsStore: P2pEndpointsStore[E],
      availabilityStore: AvailabilityStore[E],
      epochStore: EpochStore[E],
      orderedBlocksReader: OrderedBlocksReader[E],
      outputStore: OutputMetadataStore[E],
  )
}
