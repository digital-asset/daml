// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.MetricsContext.withExtraMetricLabels
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted
import com.digitalasset.canton.metrics.IndexerMetrics
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.TransactionNodeStatistics

object UpdateToMeteringDbDto {

  def apply(
      clock: () => Long = () => Timestamp.now().micros,
      excludedPackageIds: Set[Ref.PackageId],
      metrics: IndexerMetrics,
  )(implicit
      mc: MetricsContext
  ): Iterable[(Offset, Update)] => Vector[DbDto.TransactionMetering] = input => {

    val time = clock()

    if (input.nonEmpty) {

      @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
      val ledgerOffset: Long = input.last._1.unwrap

      (for {
        (completionInfo, transactionAccepted) <- input.iterator
          .collect { case (_, ta: TransactionAccepted) => ta }
          .flatMap(ta => ta.completionInfoO.iterator.map(_ -> ta))
        userId = completionInfo.userId
        statistics = TransactionNodeStatistics(transactionAccepted.transaction, excludedPackageIds)
      } yield (userId, statistics.committed.actions + statistics.rolledBack.actions)).toList
        .groupMapReduce(_._1)(_._2)(_ + _)
        .toList
        .filter(_._2 != 0)
        .sortBy(_._1)
        .map { case (userId, count) =>
          withExtraMetricLabels(IndexerMetrics.Labels.userId -> userId) { implicit mc =>
            metrics.meteredEventsMeter.mark(count.toLong)
          }
          DbDto.TransactionMetering(
            user_id = userId,
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
