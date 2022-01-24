// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.CompletionInfo
import com.daml.ledger.participant.state.v2.Update.TransactionAccepted
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp

object UpdateToMeteringDbDto {

  def apply(clock: () => Long = () => Timestamp.now().micros): Iterator[(Offset, state.Update)] => Vector[DbDto.TransactionMetering] = input => {

    val time = clock()

    val metering = input
      .collect { case (offset, ta: TransactionAccepted) =>
        (offset, ta.optCompletionInfo)
      }
      .collect({
        case (offset, Some(CompletionInfo(_, applicationId, _, _, _, Some(statistics)))) =>
          (offset, applicationId, statistics)
      })
      .foldLeft(Map.empty[Ref.ApplicationId, DbDto.TransactionMetering]) {
        case (map, (offset, applicationId, statistics)) =>
          val update = map.get(applicationId) match {
            case None =>
              DbDto.TransactionMetering(
                applicationId,
                statistics.committed.actions + statistics.rolledBack.actions,
                time,
                time,
                offset.toHexString,
                offset.toHexString,
              )
            case Some(m) =>
              m.copy(
                action_count = m.action_count + statistics.committed.actions + statistics.rolledBack.actions,
                to_timestamp = time,
                to_ledger_offset = offset.toHexString,
              )
          }
          map + (applicationId -> update)
      }
    metering.values.toVector.sortBy(_.application_id)
  }
}
