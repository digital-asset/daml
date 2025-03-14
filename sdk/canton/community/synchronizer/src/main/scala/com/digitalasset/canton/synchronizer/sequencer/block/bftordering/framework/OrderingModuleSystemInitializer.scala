// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.OrderingModuleSystemInitializer.ModuleFactories
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Mempool,
  Output,
  P2PNetworkIn,
  P2PNetworkOut,
  Pruning,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveRequest

import Module.{SystemInitializationResult, SystemInitializer}

/** A module system initializer for the general ordering system based on module factories.
  */
class OrderingModuleSystemInitializer[E <: Env[E]](moduleFactories: ModuleFactories[E])
    extends SystemInitializer[E, BftOrderingServiceReceiveRequest, Mempool.Message] {
  override def initialize(
      moduleSystem: ModuleSystem[E],
      p2pNetworkManager: ClientP2PNetworkManager[E, BftOrderingServiceReceiveRequest],
  ): SystemInitializationResult[BftOrderingServiceReceiveRequest, Mempool.Message] = {
    val mempoolRef = moduleSystem.newModuleRef[Mempool.Message](ModuleName("mempool"))
    val p2pNetworkInRef =
      moduleSystem.newModuleRef[BftOrderingServiceReceiveRequest](ModuleName("p2pnetwork-in"))
    val p2pNetworkOutRef =
      moduleSystem.newModuleRef[P2PNetworkOut.Message](ModuleName("p2p-network-out"))
    val availabilityRef =
      moduleSystem.newModuleRef[Availability.Message[E]](ModuleName("availability"))
    val consensusRef = moduleSystem.newModuleRef[Consensus.Message[E]](ModuleName("consensus"))
    val outputRef = moduleSystem.newModuleRef[Output.Message[E]](ModuleName("output"))
    val pruningRef = moduleSystem.newModuleRef[Pruning.Message](ModuleName("pruning"))

    val mempool = moduleFactories.mempool(availabilityRef)
    val p2pNetworkIn = moduleFactories.p2pNetworkIn(availabilityRef, consensusRef)
    val p2pNetworkOut = moduleFactories.p2pNetworkOut(
      p2pNetworkManager,
      p2pNetworkInRef,
      mempoolRef,
      availabilityRef,
      consensusRef,
      outputRef,
    )
    val availability = moduleFactories.availability(
      mempoolRef,
      p2pNetworkOutRef,
      consensusRef,
      outputRef,
    )
    val output = moduleFactories.output(availabilityRef, consensusRef)
    val consensus =
      moduleFactories.consensus(p2pNetworkOutRef, availabilityRef, outputRef)

    val pruning = moduleFactories.pruning()

    moduleSystem.setModule(mempoolRef, mempool)
    moduleSystem.setModule(p2pNetworkOutRef, p2pNetworkOut)
    moduleSystem.setModule(p2pNetworkInRef, p2pNetworkIn)
    moduleSystem.setModule(availabilityRef, availability)
    moduleSystem.setModule(consensusRef, consensus)
    moduleSystem.setModule(outputRef, output)
    moduleSystem.setModule(pruningRef, pruning)

    mempool.ready(self = mempoolRef)
    p2pNetworkIn.ready(self = p2pNetworkInRef)
    p2pNetworkOut.ready(self = p2pNetworkOutRef)
    availability.ready(self = availabilityRef)
    consensus.ready(self = consensusRef)
    output.ready(self = outputRef)
    pruning.ready(self = pruningRef)

    SystemInitializationResult(
      mempoolRef,
      p2pNetworkInRef,
      p2pNetworkOutRef,
      consensusRef,
      outputRef,
    )
  }
}

object OrderingModuleSystemInitializer {
  final case class ModuleFactories[E <: Env[E]](
      mempool: ModuleRef[Availability.Message[E]] => Mempool[E],
      p2pNetworkIn: (
          ModuleRef[Availability.Message[E]],
          ModuleRef[Consensus.Message[E]],
      ) => P2PNetworkIn[E],
      p2pNetworkOut: (
          ClientP2PNetworkManager[E, BftOrderingServiceReceiveRequest],
          ModuleRef[BftOrderingServiceReceiveRequest],
          ModuleRef[Mempool.Message],
          ModuleRef[Availability.Message[E]],
          ModuleRef[Consensus.Message[E]],
          ModuleRef[Output.Message[E]],
      ) => P2PNetworkOut[E],
      availability: (
          ModuleRef[Mempool.Message],
          ModuleRef[P2PNetworkOut.Message],
          ModuleRef[Consensus.Message[E]],
          ModuleRef[Output.Message[E]],
      ) => Availability[E],
      consensus: (
          ModuleRef[P2PNetworkOut.Message],
          ModuleRef[Availability.Message[E]],
          ModuleRef[Output.Message[E]],
      ) => Consensus[E],
      output: (
          ModuleRef[Availability.Message[E]],
          ModuleRef[Consensus.Message[E]],
      ) => Output[E],
      pruning: () => Pruning[E],
  )
}
