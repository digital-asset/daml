// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.pruning.admin.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{config, participant, scheduler}

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

  private[data] def fromInternal(
      internalSchedule: scheduler.PruningSchedule
  ): PruningSchedule =
    PruningSchedule(
      internalSchedule.cron.toProtoPrimitive,
      config.PositiveDurationSeconds(internalSchedule.maxDuration.toScala),
      config.PositiveDurationSeconds(internalSchedule.retention.toScala),
    )
}

final case class ParticipantPruningSchedule(
    schedule: PruningSchedule,
    pruneInternallyOnly: Boolean,
)

object ParticipantPruningSchedule {
  private[admin] def fromProtoV0(
      participantSchedule: v0.ParticipantPruningSchedule
  ): ParsingResult[ParticipantPruningSchedule] =
    for {
      internalSchedule <- participant.scheduler.ParticipantPruningSchedule.fromProtoV0(
        participantSchedule
      )
    } yield ParticipantPruningSchedule(
      PruningSchedule.fromInternal(internalSchedule.schedule),
      participantSchedule.pruneInternallyOnly,
    )
}
