// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.option.*
import com.digitalasset.canton.admin.api.client.commands.EnterpriseSequencerAdminCommands
import com.digitalasset.canton.admin.api.client.commands.EnterpriseSequencerAdminCommands.InitializeX
import com.digitalasset.canton.admin.api.client.data.StaticDomainParameters
import com.digitalasset.canton.console.Help
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.admin.grpc.InitializeSequencerResponseX
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionX,
  StoredTopologyTransactionsX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.PositiveSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.{TopologyChangeOpX, TopologyMappingX}

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
