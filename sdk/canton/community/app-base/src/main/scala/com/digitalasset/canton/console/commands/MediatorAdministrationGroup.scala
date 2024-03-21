// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.EnterpriseMediatorAdministrationCommands.{
  Initialize,
  LocatePruningTimestampCommand,
  Prune,
}
import com.digitalasset.canton.admin.api.client.commands.{
  DomainTimeCommands,
  PruningSchedulerCommands,
}
import com.digitalasset.canton.admin.api.client.data.StaticDomainParameters
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
}
import com.digitalasset.canton.crypto.{Fingerprint, PublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0.EnterpriseMediatorAdministrationServiceGrpc
import com.digitalasset.canton.domain.admin.v0.EnterpriseMediatorAdministrationServiceGrpc.EnterpriseMediatorAdministrationServiceStub
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.{SequencerConnection, SequencerConnections}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.topology.{DomainId, MediatorId}

import scala.concurrent.duration.FiniteDuration

class MediatorTestingGroup(
    runner: AdminCommandRunner,
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends FeatureFlagFilter
    with Helpful {

  @Help.Summary("Fetch the current time from the domain", FeatureFlag.Testing)
  def fetch_domain_time(
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.ledgerCommand
  ): CantonTimestamp =
    check(FeatureFlag.Testing) {
      consoleEnvironment.run {
        runner.adminCommand(
          DomainTimeCommands.FetchTime(None, NonNegativeFiniteDuration.Zero, timeout)
        )
      }.timestamp
    }

  @Help.Summary("Await for the given time to be reached on the domain", FeatureFlag.Testing)
  def await_domain_time(time: CantonTimestamp, timeout: NonNegativeDuration): Unit =
    check(FeatureFlag.Testing) {
      consoleEnvironment.run {
        runner.adminCommand(
          DomainTimeCommands.AwaitTime(None, time, timeout)
        )
      }
    }
}

class MediatorPruningAdministrationGroup(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends PruningSchedulerAdministration(
      runner,
      consoleEnvironment,
      new PruningSchedulerCommands[EnterpriseMediatorAdministrationServiceStub](
        EnterpriseMediatorAdministrationServiceGrpc.stub,
        _.setSchedule(_),
        _.clearSchedule(_),
        _.setCron(_),
        _.setMaxDuration(_),
        _.setRetention(_),
        _.getSchedule(_),
      ),
      loggerFactory,
    )
    with Helpful {

  @Help.Summary(
    "Prune the mediator of unnecessary data while keeping data for the default retention period"
  )
  @Help.Description(
    """Removes unnecessary data from the Mediator that is earlier than the default retention period.
          |The default retention period is set in the configuration of the canton node running this
          |command under `parameters.retention-period-defaults.mediator`."""
  )
  def prune(): Unit = {
    val defaultRetention =
      consoleEnvironment.environment.config.parameters.retentionPeriodDefaults.mediator
    prune_with_retention_period(defaultRetention.underlying)
  }

  @Help.Summary(
    "Prune the mediator of unnecessary data while keeping data for the provided retention period"
  )
  def prune_with_retention_period(retentionPeriod: FiniteDuration): Unit = {
    import scala.jdk.DurationConverters.*
    val pruneUpTo = consoleEnvironment.environment.clock.now.minus(retentionPeriod.toJava)
    prune_at(pruneUpTo)
  }

  @Help.Summary("Prune the mediator of unnecessary data up to and including the given timestamp")
  def prune_at(timestamp: CantonTimestamp): Unit = consoleEnvironment.run {
    runner.adminCommand(Prune(timestamp))
  }

  @Help.Summary("Obtain a timestamp at or near the beginning of mediator state")
  @Help.Description(
    """This command provides insight into the current state of mediator pruning when called with
      |the default value of `index` 1.
      |When pruning the mediator manually via `prune_at` and with the intent to prune in batches, specify
      |a value such as 1000 to obtain a pruning timestamp that corresponds to the "end" of the batch."""
  )
  def locate_pruning_timestamp(
      index: PositiveInt = PositiveInt.tryCreate(1)
  ): Option[CantonTimestamp] =
    consoleEnvironment.run {
      runner.adminCommand(LocatePruningTimestampCommand(index))
    }

}

class MediatorAdministrationGroup(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends MediatorPruningAdministrationGroup(runner, consoleEnvironment, loggerFactory) {

  private lazy val testing_ = new MediatorTestingGroup(runner, consoleEnvironment, loggerFactory)
  @Help.Summary("Testing functionality for the mediator")
  @Help.Group("Testing")
  def testing: MediatorTestingGroup = testing_

}

@Help.Summary("Manage the mediator component")
@Help.Group("Mediator")
class MediatorAdministrationGroupWithInit(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends MediatorAdministrationGroup(runner, consoleEnvironment, loggerFactory) {

  @Help.Summary("Initialize a mediator")
  def initialize(
      domainId: DomainId,
      mediatorId: MediatorId,
      domainParameters: StaticDomainParameters,
      sequencerConnections: SequencerConnections,
      topologySnapshot: Option[StoredTopologyTransactions[TopologyChangeOp.Positive]],
      signingKeyFingerprint: Option[Fingerprint] = None,
  ): PublicKey = consoleEnvironment.run {
    runner.adminCommand(
      Initialize(
        domainId,
        mediatorId,
        topologySnapshot,
        domainParameters.toInternal,
        sequencerConnections,
        signingKeyFingerprint,
      )
    )
  }

  @Help.Summary("Initialize a mediator")
  def initialize(
      domainId: DomainId,
      mediatorId: MediatorId,
      domainParameters: StaticDomainParameters,
      sequencerConnection: SequencerConnection,
      topologySnapshot: Option[StoredTopologyTransactions[TopologyChangeOp.Positive]],
      signingKeyFingerprint: Option[Fingerprint],
  ): PublicKey = consoleEnvironment.run {
    runner.adminCommand(
      Initialize(
        domainId,
        mediatorId,
        topologySnapshot,
        domainParameters.toInternal,
        SequencerConnections.single(sequencerConnection),
        signingKeyFingerprint,
      )
    )
  }
}
