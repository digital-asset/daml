// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.SequencerBftPruningAdminCommands.GetBftSchedule
import com.digitalasset.canton.admin.api.client.commands.{
  PruningSchedulerCommands,
  SequencerBftPruningAdminCommands,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.PositiveDurationSeconds
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlagFilter,
  Help,
  Helpful,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencer.admin.v30.SequencerBftPruningAdministrationServiceGrpc
import com.digitalasset.canton.sequencer.admin.v30.SequencerBftPruningAdministrationServiceGrpc.SequencerBftPruningAdministrationServiceStub
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.{
  BftOrdererPruningSchedule,
  BftPruningStatus,
}

class SequencerBftPruningAdministrationGroup(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends PruningSchedulerAdministration(
      runner,
      consoleEnvironment,
      new PruningSchedulerCommands[SequencerBftPruningAdministrationServiceStub](
        SequencerBftPruningAdministrationServiceGrpc.stub,
        _.setSchedule(_),
        _.clearSchedule(_),
        _.setCron(_),
        _.setMaxDuration(_),
        _.setRetention(_),
        _.getSchedule(_),
      ),
      loggerFactory,
    )
    with FeatureFlagFilter
    with Helpful {

  @Help.Summary(
    "Prune the BFT Orderer layer based on the retention period and minimum blocks to keep specified"
  )
  @Help.Description(
    """Prunes the BFT Orderer layer based on the retention period and minimum blocks to keep
      |specified returning a description of how the operation went.
      """
  )
  def prune(retention: PositiveDurationSeconds, minBlocksToKeep: Int): String =
    consoleEnvironment.run {
      runner.adminCommand(SequencerBftPruningAdminCommands.Prune(retention, minBlocksToKeep))
    }

  @Help.Summary("Pruning status of the BFT Orderer")
  @Help.Description(
    """Provides a detailed breakdown of information required for pruning:
      |- the latest block number
      |- the latest block epoch number
      |- the latest block timestamp
      |- the lower bound inclusive epoch number it current supports serving from
      |- the lower bound inclusive block number it current supports serving from
      """
  )
  def status(): BftPruningStatus =
    consoleEnvironment.run {
      runner.adminCommand(SequencerBftPruningAdminCommands.Status())
    }

  @Help.Summary(
    "Activate automatic pruning according to the specified schedule with bft-orderer-specific options"
  )
  @Help.Description(
    """Refer to the ``set_schedule`` description for information about the "cron",
      |"max_duration", and "retention" parameters. The "min-blocks-to-keep" option refers to how
      |many blocks should be kept at a minimum after the pruning operation ends, even if fewer
      |blocks would remain based on retention alone.
      """
  )
  def set_bft_schedule(
      cron: String,
      maxDuration: config.PositiveDurationSeconds,
      retention: config.PositiveDurationSeconds,
      minBlocksToKeep: Int = BftOrdererPruningSchedule.DefaultMinNumberOfBlocksToKeep,
  ): Unit =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerBftPruningAdminCommands.SetBftSchedule(
          cron,
          maxDuration,
          retention,
          minBlocksToKeep,
        )
      )
    )

  @Help.Summary("Inspect the automatic, bft-orderer-specific pruning schedule")
  @Help.Description(
    """The schedule consists of a "cron" expression and "max_duration" and "retention"
      |durations as described in the ``get_schedule`` command description. Additionally
      |"min_blocks_to_keep" indicates how many blocks should be kept at a minimum after the
      |pruning operation ends, even if fewer blocks would remain based on retention alone.
      """
  )
  def get_bft_schedule(): Option[BftOrdererPruningSchedule] =
    consoleEnvironment.run(
      runner.adminCommand(GetBftSchedule())
    )

  @Help.Summary(
    "Set min-blocks-to-keep parameter of automatic pruning schedule for the bft orderer"
  )
  def set_min_blocks_to_keep(
      minBlocksToKeep: Int
  ): Unit = consoleEnvironment.run(
    runner.adminCommand(
      SequencerBftPruningAdminCommands.SetMinBlocksToKeep(
        minBlocksToKeep
      )
    )
  )

}
