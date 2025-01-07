// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import com.digitalasset.canton.admin.pruning.v30
import com.digitalasset.canton.scheduler.PruningSchedule
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** The participant pruning schedule that includes a flag whether to prune only internally.
  */
final case class ParticipantPruningSchedule(
    schedule: PruningSchedule,
    pruneInternallyOnly: Boolean,
) {
  def toProtoV30: v30.ParticipantPruningSchedule = v30.ParticipantPruningSchedule(
    schedule = Some(schedule.toProtoV30),
    pruneInternallyOnly = pruneInternallyOnly,
  )
}

object ParticipantPruningSchedule {

  def fromProtoV30(
      participantSchedule: v30.ParticipantPruningSchedule
  ): ParsingResult[ParticipantPruningSchedule] =
    for {
      scheduleP <- ProtoConverter.required("schedule", participantSchedule.schedule)
      schedule <- PruningSchedule.fromProtoV30(scheduleP)
    } yield ParticipantPruningSchedule(
      schedule,
      participantSchedule.pruneInternallyOnly,
    )

  def fromPruningSchedule(schedule: PruningSchedule): ParticipantPruningSchedule =
    // By default pruning-internally-only is disabled as "incomplete pruning" of a canton
    // participant needs to be done deliberately in other not to run out of storage space.
    ParticipantPruningSchedule(schedule, pruneInternallyOnly = false)
}
