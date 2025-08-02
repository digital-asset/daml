// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import com.digitalasset.canton.scheduler.PruningSchedule
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class BftOrdererPruningSchedule(
    schedule: PruningSchedule,
    minBlocksToKeep: Int,
) {
  def toProtoV30: v30.BftOrdererPruningSchedule = v30.BftOrdererPruningSchedule(
    schedule = Some(schedule.toProtoV30),
    minBlocksToKeep = minBlocksToKeep,
  )
}

object BftOrdererPruningSchedule {
  def fromProtoV30(
      bftOrdererSchedule: v30.BftOrdererPruningSchedule
  ): ParsingResult[BftOrdererPruningSchedule] =
    for {
      scheduleP <- ProtoConverter.required("schedule", bftOrdererSchedule.schedule)
      schedule <- PruningSchedule.fromProtoV30(scheduleP)
    } yield BftOrdererPruningSchedule(
      schedule,
      bftOrdererSchedule.minBlocksToKeep,
    )

  def fromPruningSchedule(schedule: PruningSchedule): BftOrdererPruningSchedule =
    BftOrdererPruningSchedule(schedule, DefaultMinNumberOfBlocksToKeep)

  val DefaultMinNumberOfBlocksToKeep = 100
}
