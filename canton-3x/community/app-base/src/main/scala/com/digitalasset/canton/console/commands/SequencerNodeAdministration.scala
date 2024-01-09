// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.option.*
import com.digitalasset.canton.admin.api.client.commands.EnterpriseSequencerAdminCommands
import com.digitalasset.canton.admin.api.client.commands.EnterpriseSequencerAdminCommands.{
  BootstrapTopology,
  Initialize,
  InitializeX,
}
import com.digitalasset.canton.admin.api.client.data.StaticDomainParameters
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  FeatureFlagFilter,
  Help,
  Helpful,
  SequencerNodeReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.admin.grpc.{
  InitializeSequencerResponse,
  InitializeSequencerResponseX,
}
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionX,
  StoredTopologyTransactions,
  StoredTopologyTransactionsX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.PositiveSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.{
  TopologyChangeOp,
  TopologyChangeOpX,
  TopologyMappingX,
}

trait SequencerNodeAdministration {
  self: AdminCommandRunner with FeatureFlagFilter with SequencerNodeReference =>

  private lazy val _init = new Initialization()

  def initialization: Initialization = _init

  @Help.Summary("Manage sequencer initialization")
  @Help.Group("initialization")
  class Initialization extends Helpful {
    @Help.Summary(
      "Initialize a sequencer from the beginning of the event stream. This should only be called for " +
        "sequencer nodes being initialized at the same time as the corresponding domain node. " +
        "This is called as part of the domain.setup.bootstrap command, so you are unlikely to need to call this directly."
    )
    def initialize_from_beginning(
        domainId: DomainId,
        domainParameters: StaticDomainParameters,
    ): InitializeSequencerResponse =
      consoleEnvironment.run {
        adminCommand(
          Initialize(domainId, StoredTopologyTransactions.empty, domainParameters)
        )
      }

    @Help.Summary(
      "Dynamically initialize a sequencer from a point later than the beginning of the event stream." +
        "This is called as part of the domain.setup.onboard_new_sequencer command, so you are unlikely to need to call this directly."
    )
    def initialize_from_snapshot(
        domainId: DomainId,
        topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive],
        sequencerSnapshot: SequencerSnapshot,
        domainParameters: StaticDomainParameters,
    ): InitializeSequencerResponse =
      consoleEnvironment.run {
        adminCommand(
          Initialize(domainId, topologySnapshot, domainParameters, sequencerSnapshot.some)
        )
      }

    @Help.Summary("Bootstrap topology data")
    @Help.Description(
      "Use this to sequence the initial batch of topology transactions which must include at least the IDM's and sequencer's" +
        "key mappings. This is called as part of domain.setup.bootstrap"
    )
    def bootstrap_topology(
        topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive]
    ): Unit =
      consoleEnvironment.run {
        adminCommand(BootstrapTopology(topologySnapshot))
      }
  }
}

class SequencerXSetupGroup(parent: ConsoleCommandGroup)
    extends ConsoleCommandGroup.Impl(parent)
    with InitNodeId {

  @Help.Summary(
    "Download sequencer snapshot at given point in time to bootstrap another sequencer"
  )
  def snapshot(timestamp: CantonTimestamp): SequencerSnapshot = {
    // TODO(#14074) add something like "snapshot for sequencer-id", rather than timestamp based
    //      we still need to keep the timestamp based such that we can provide recovery for corrupted sequencers
    consoleEnvironment.run {
      runner.adminCommand(EnterpriseSequencerAdminCommands.Snapshot(timestamp))
    }
  }

  @Help.Summary(
    "Initialize a sequencer from the beginning of the event stream. This should only be called for " +
      "sequencer nodes being initialized at the same time as the corresponding domain node. " +
      "This is called as part of the domain.setup.bootstrap command, so you are unlikely to need to call this directly."
  )
  def assign_from_beginning(
      genesisState: Seq[PositiveSignedTopologyTransactionX],
      domainParameters: StaticDomainParameters,
  ): InitializeSequencerResponseX =
    consoleEnvironment.run {
      runner.adminCommand(
        InitializeX(
          StoredTopologyTransactionsX[TopologyChangeOpX.Replace, TopologyMappingX](
            genesisState.map(signed =>
              StoredTopologyTransactionX(
                SequencedTime(CantonTimestamp.MinValue.immediateSuccessor),
                EffectiveTime(CantonTimestamp.MinValue.immediateSuccessor),
                None,
                signed,
              )
            )
          ),
          domainParameters.toInternal,
          None,
        )
      )
    }

  @Help.Summary(
    "Dynamically initialize a sequencer from a point later than the beginning of the event stream." +
      "This is called as part of the domain.setup.onboard_new_sequencer command, so you are unlikely to need to call this directly."
  )
  def assign_from_snapshot(
      topologySnapshot: GenericStoredTopologyTransactionsX,
      sequencerSnapshot: SequencerSnapshot,
      domainParameters: StaticDomainParameters,
  ): InitializeSequencerResponseX =
    consoleEnvironment.run {
      runner.adminCommand(
        InitializeX(topologySnapshot, domainParameters.toInternal, sequencerSnapshot.some)
      )
    }

}

trait SequencerNodeAdministrationGroupXWithInit extends SequencerAdministrationGroupX {

  private lazy val setup_ = new SequencerXSetupGroup(this)
  @Help.Summary("Methods used for node initialization")
  def setup: SequencerXSetupGroup = setup_
}
