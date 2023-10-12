// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.config
import com.digitalasset.canton.pruning.admin.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class PruningSchedule(
    cron: String,
    maxDuration: config.PositiveDurationSeconds,
    retention: config.PositiveDurationSeconds,
)

object PruningSchedule {
  private[admin] def fromProtoV0(scheduleP: v0.PruningSchedule): ParsingResult[PruningSchedule] =
    for {
      maxDuration <- config.PositiveDurationSeconds.fromProtoPrimitiveO("max_duration")(
        scheduleP.maxDuration
      )
      retention <- config.PositiveDurationSeconds.fromProtoPrimitiveO("retention")(
        scheduleP.retention
      )
    } yield PruningSchedule(scheduleP.cron, maxDuration, retention)
}
