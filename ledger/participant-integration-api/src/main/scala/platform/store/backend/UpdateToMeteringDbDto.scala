// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update.TransactionAccepted
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Time.Timestamp

object UpdateToMeteringDbDto {

  def apply(
      clock: () => Long = () => Timestamp.now().micros
  ): Iterable[(Offset, state.Update)] => Vector[DbDto.TransactionMetering] = input => {

    val time = clock()

    if (input.nonEmpty) {

      val ledgerOffset = input.last._1.toHexString

      (for {
        optCompletionInfo <- input.collect { case (_, ta: TransactionAccepted) =>
          ta.optCompletionInfo
        }
        ci <- optCompletionInfo.iterator
        statistics <- ci.statistics
      } yield (ci.applicationId, statistics.committed.actions + statistics.rolledBack.actions))
        .groupMapReduce(_._1)(_._2)(_ + _)
        .toList
        .sortBy(_._1)
        .map { case (applicationId, count) =>
          DbDto.TransactionMetering(
            application_id = applicationId,
            action_count = count,
            metering_timestamp = time,
            ledger_offset = ledgerOffset,
          )
        }
        .toVector
    } else {
      Vector.empty[DbDto.TransactionMetering]
    }
  }
}
