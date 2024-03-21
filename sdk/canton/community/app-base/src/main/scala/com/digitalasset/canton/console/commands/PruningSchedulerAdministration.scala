// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.PruningSchedulerCommands
import com.digitalasset.canton.admin.api.client.data.PruningSchedule
import com.digitalasset.canton.config.PositiveDurationSeconds
import com.digitalasset.canton.console.{AdminCommandRunner, ConsoleEnvironment, Help, Helpful}
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.grpc.stub.AbstractStub

/** Pruning scheduler administration api shared by participant/mediator/sequencer.
  */
class PruningSchedulerAdministration[T <: AbstractStub[T]](
    runner: AdminCommandRunner,
    protected val consoleEnvironment: ConsoleEnvironment,
    commands: PruningSchedulerCommands[T],
    protected val loggerFactory: NamedLoggerFactory,
) extends Helpful {

  @Help.Summary(
    "Activate automatic pruning according to the specified schedule."
  )
  @Help.Description(
    """The schedule is specified in cron format and "max_duration" and "retention" durations. The cron string indicates
      |the points in time at which pruning should begin in the GMT time zone, and the maximum duration indicates how
      |long from the start time pruning is allowed to run as long as pruning has not finished pruning up to the
      |specified retention period.
    """
  )
  def set_schedule(
      cron: String,
      maxDuration: PositiveDurationSeconds,
      retention: PositiveDurationSeconds,
  ): Unit =
    consoleEnvironment.run(
      runner.adminCommand(
        commands.SetScheduleCommand(cron = cron, maxDuration = maxDuration, retention = retention)
      )
    )

  @Help.Summary("Deactivate automatic pruning.")
  def clear_schedule(): Unit =
    consoleEnvironment.run(
      runner.adminCommand(commands.ClearScheduleCommand())
    )

  @Help.Summary("Modify the cron used by automatic pruning.")
  @Help.Description(
    """The schedule is specified in cron format and refers to pruning start times in the GMT time zone.
      |This call returns an error if no schedule has been configured via `set_schedule` or if automatic
      |pruning has been disabled via `clear_schedule`. Additionally if at the time of this modification, pruning is
      |actively running, a best effort is made to pause pruning and restart according to the new schedule. This
      |allows for the case that the new schedule no longer allows pruning at the current time.
    """
  )
  def set_cron(cron: String): Unit =
    consoleEnvironment.run(
      runner.adminCommand(commands.SetCronCommand(cron))
    )

  @Help.Summary("Modify the maximum duration used by automatic pruning.")
  @Help.Description(
    """The `maxDuration` is specified as a positive duration and has at most per-second granularity.
      |This call returns an error if no schedule has been configured via `set_schedule` or if automatic
      |pruning has been disabled via `clear_schedule`. Additionally if at the time of this modification, pruning is
      |actively running, a best effort is made to pause pruning and restart according to the new schedule. This
      |allows for the case that the new schedule no longer allows pruning at the current time.
    """
  )
  def set_max_duration(maxDuration: PositiveDurationSeconds): Unit =
    consoleEnvironment.run(
      runner.adminCommand(
        commands.SetMaxDurationCommand(maxDuration)
      )
    )

  @Help.Summary("Update the pruning retention used by automatic pruning.")
  @Help.Description(
    """The `retention` is specified as a positive duration and has at most per-second granularity.
      |This call returns an error if no schedule has been configured via `set_schedule` or if automatic
      |pruning has been disabled via `clear_schedule`. Additionally if at the time of this update, pruning is
      |actively running, a best effort is made to pause pruning and restart with the newly specified retention.
      |This allows for the case that the new retention mandates retaining more data than previously.
    """
  )
  def set_retention(retention: PositiveDurationSeconds): Unit =
    consoleEnvironment.run(
      runner.adminCommand(
        commands.SetRetentionCommand(retention)
      )
    )

  @Help.Summary("Inspect the automatic pruning schedule.")
  @Help.Description(
    """The schedule consists of a "cron" expression and "max_duration" and "retention" durations. The cron string
      |indicates the points in time at which pruning should begin in the GMT time zone, and the maximum duration
      |indicates how long from the start time pruning is allowed to run as long as pruning has not finished pruning
      |up to the specified retention period.
      |Returns `None` if no schedule has been configured via `set_schedule` or if `clear_schedule` has been invoked.
    """
  )
  def get_schedule(): Option[PruningSchedule] =
    consoleEnvironment.run(
      runner.adminCommand(commands.GetScheduleCommand())
    )

}
