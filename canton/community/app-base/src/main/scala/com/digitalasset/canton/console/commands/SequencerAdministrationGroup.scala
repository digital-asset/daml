// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.EnterpriseSequencerAdminCommands.LocatePruningTimestampCommand
import com.digitalasset.canton.admin.api.client.commands.{
  EnterpriseSequencerAdminCommands,
  PruningSchedulerCommands,
  SequencerAdminCommands,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlag,
  Help,
  Helpful,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0.EnterpriseSequencerAdministrationServiceGrpc
import com.digitalasset.canton.domain.admin.v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub
import com.digitalasset.canton.domain.sequencing.sequencer.{
  SequencerClients,
  SequencerPruningStatus,
  SequencerSnapshot,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.EnrichedDurations.*
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*

trait SequencerAdministrationGroupCommon extends ConsoleCommandGroup {

  @Help.Summary("Pruning of the sequencer")
  object pruning
      extends PruningSchedulerAdministration(
        runner,
        consoleEnvironment,
        new PruningSchedulerCommands[EnterpriseSequencerAdministrationServiceStub](
          EnterpriseSequencerAdministrationServiceGrpc.stub,
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
    @Help.Summary("Status of the sequencer and its connected clients")
    @Help.Description(
      """Provides a detailed breakdown of information required for pruning:
        | - the current time according to this sequencer instance
        | - domain members that the sequencer supports
        | - for each member when they were registered and whether they are enabled
        | - a list of clients for each member, their last acknowledgement, and whether they are enabled
        |"""
    )
    def status(): SequencerPruningStatus =
      consoleEnvironment.run {
        runner.adminCommand(SequencerAdminCommands.GetPruningStatus)
      }

    @Help.Summary("Remove unnecessary data from the Sequencer up until the default retention point")
    @Help.Description(
      """Removes unnecessary data from the Sequencer that is earlier than the default retention period.
        |The default retention period is set in the configuration of the canton processing running this
        |command under `parameters.retention-period-defaults.sequencer`.
        |This pruning command requires that data is read and acknowledged by clients before
        |considering it safe to remove.
        |
        |If no data is being removed it could indicate that clients are not reading or acknowledging data
        |in a timely fashion (typically due to nodes going offline for long periods).
        |You have the option of disabling the members running on these nodes to allow removal of this data,
        |however this will mean that they will be unable to reconnect to the domain in the future.
        |To do this run `force_prune(dryRun = true)` to return a description of which members would be
        |disabled in order to prune the Sequencer.
        |If you are happy to disable the described clients then run `force_prune(dryRun = false)` to
        |permanently remove their unread data.
        |
        |Once offline clients have been disabled you can continue to run `prune` normally.
        |"""
    )
    def prune(): String = {
      val defaultRetention =
        consoleEnvironment.environment.config.parameters.retentionPeriodDefaults.sequencer
      prune_with_retention_period(defaultRetention.underlying)
    }

    @Help.Summary(
      "Force remove data from the Sequencer including data that may have not been read by offline clients"
    )
    @Help.Description(
      """Will force pruning up until the default retention period by potentially disabling clients
        |that have not yet read data we would like to remove.
        |Disabling these clients will prevent them from ever reconnecting to the Domain so should only be
        |used if the Domain operator is confident they can be permanently ignored.
        |Run with `dryRun = true` to review a description of which clients will be disabled first.
        |Run with `dryRun = false` to disable these clients and perform a forced pruning.
        |"""
    )
    def force_prune(dryRun: Boolean): String = {
      val defaultRetention =
        consoleEnvironment.environment.config.parameters.retentionPeriodDefaults.sequencer
      force_prune_with_retention_period(defaultRetention.underlying, dryRun)
    }

    @Help.Summary("Remove data that has been read up until a custom retention period")
    @Help.Description(
      "Similar to the above `prune` command but allows specifying a custom retention period"
    )
    def prune_with_retention_period(retentionPeriod: FiniteDuration): String = {
      val status = this.status()
      val pruningTimestamp = status.now.minus(retentionPeriod.toJava)

      prune_at(pruningTimestamp)
    }

    @Help.Summary(
      "Force removing data from the Sequencer including data that may have not been read by offline clients up until a custom retention period"
    )
    @Help.Description(
      "Similar to the above `force_prune` command but allows specifying a custom retention period"
    )
    def force_prune_with_retention_period(
        retentionPeriod: FiniteDuration,
        dryRun: Boolean,
    ): String = {
      val status = this.status()
      val pruningTimestamp = status.now.minus(retentionPeriod.toJava)

      force_prune_at(pruningTimestamp, dryRun)
    }

    @Help.Summary("Remove data that has been read up until the specified time")
    @Help.Description(
      """Similar to the above `prune` command but allows specifying the exact time at which to prune.
        |The command will fail if a client has not yet read and acknowledged some data up to the specified time."""
    )
    def prune_at(timestamp: CantonTimestamp): String = {
      val status = this.status()
      val unauthenticatedMembers =
        status.unauthenticatedMembersToDisable(
          consoleEnvironment.environment.config.parameters.retentionPeriodDefaults.unauthenticatedMembers.toInternal
        )
      unauthenticatedMembers.foreach(disable_member)
      val msg = consoleEnvironment.run {
        runner.adminCommand(EnterpriseSequencerAdminCommands.Prune(timestamp))
      }
      s"$msg. Automatically disabled ${unauthenticatedMembers.size} unauthenticated member clients."
    }

    @Help.Summary(
      "Force removing data from the Sequencer including data that may have not been read by offline clients up until the specified time"
    )
    @Help.Description(
      "Similar to the above `force_prune` command but allows specifying the exact time at which to prune"
    )
    def force_prune_at(timestamp: CantonTimestamp, dryRun: Boolean): String = {
      val initialStatus = status()
      val clientsToDisable = initialStatus.clientsPreventingPruning(timestamp)

      if (dryRun) {
        formatDisableDryRun(timestamp, clientsToDisable)
      } else {
        disableClients(clientsToDisable)

        // check we can now prune for the provided timestamp
        val statusAfterDisabling = status()
        val safeTimestamp = statusAfterDisabling.safePruningTimestamp

        if (safeTimestamp < timestamp)
          sys.error(
            s"We disabled all clients preventing pruning at $timestamp however the safe timestamp is set to $safeTimestamp"
          )

        prune_at(timestamp)
      }
    }

    private def disableClients(toDisable: SequencerClients): Unit =
      toDisable.members.foreach(disable_member)

    private def formatDisableDryRun(
        timestamp: CantonTimestamp,
        toDisable: SequencerClients,
    ): String = {
      val toDisableText =
        toDisable.members.toSeq.map(member => show"- $member").map(m => s"  $m (member)").sorted

      if (toDisableText.isEmpty) {
        show"The Sequencer can be safely pruned for $timestamp without disabling clients"
      } else {
        val sb = new StringBuilder()
        sb.append(s"To prune the Sequencer at $timestamp we will disable:")
        toDisableText foreach { item =>
          sb.append(System.lineSeparator())
          sb.append(item)
        }
        sb.append(System.lineSeparator())
        sb.append(
          "To disable these clients to allow for pruning at this point run force_prune with dryRun set to false"
        )
        sb.toString()
      }
    }

    @Help.Summary("Obtain a timestamp at or near the beginning of sequencer state")
    @Help.Description(
      """This command provides insight into the current state of sequencer pruning when called with
        |the default value of `index` 1.
        |When pruning the sequencer manually via `prune_at` and with the intent to prune in batches, specify
        |a value such as 1000 to obtain a pruning timestamp that corresponds to the "end" of the batch."""
    )
    def locate_pruning_timestamp(
        index: PositiveInt = PositiveInt.tryCreate(1)
    ): Option[CantonTimestamp] =
      check(FeatureFlag.Preview) {
        consoleEnvironment.run {
          runner.adminCommand(LocatePruningTimestampCommand(index))
        }
      }

  }

  protected def disable_member(member: Member): Unit

}

trait SequencerAdministrationDisableMember extends ConsoleCommandGroup {

  /** Disable the provided member at the sequencer preventing them from reading and writing, and allowing their
    * data to be pruned.
    */
  @Help.Summary(
    "Disable the provided member at the Sequencer that will allow any unread data for them to be removed"
  )
  @Help.Description("""This will prevent any client for the given member to reconnect the Sequencer
                      |and allow any unread/unacknowledged data they have to be removed.
                      |This should only be used if the domain operation is confident the member will never need
                      |to reconnect as there is no way to re-enable the member.
                      |To view members using the sequencer run `sequencer.status()`."""")
  def disable_member(member: Member): Unit = consoleEnvironment.run {
    runner.adminCommand(EnterpriseSequencerAdminCommands.DisableMember(member))
  }
}

class SequencerAdministrationGroup(
    val runner: AdminCommandRunner,
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends SequencerAdministrationGroupCommon
    with SequencerAdministrationDisableMember {

  /** Snapshot based on given snapshot to used as initial state by other sequencer nodes in the process of onboarding.
    */
  def snapshot(timestamp: CantonTimestamp): SequencerSnapshot =
    consoleEnvironment.run {
      runner.adminCommand(EnterpriseSequencerAdminCommands.Snapshot(timestamp))
    }

}

trait SequencerAdministrationGroupX extends SequencerAdministrationGroupCommon {

  @Help.Summary("Methods used for repairing the node")
  object repair extends ConsoleCommandGroup.Impl(this) with SequencerAdministrationDisableMember {}

}
