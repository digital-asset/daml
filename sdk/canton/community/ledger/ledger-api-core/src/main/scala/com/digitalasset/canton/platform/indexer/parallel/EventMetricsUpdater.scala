// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.metrics.api.MetricsContext.withExtraMetricLabels
import com.daml.metrics.api.{MetricHandle, MetricsContext}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted
import com.digitalasset.canton.metrics.IndexerMetrics
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.TransactionNodeStatistics

object EventMetricsUpdater {
  def apply(
      meteredEventsMeter: MetricHandle.Meter
  )(implicit
      mc: MetricsContext
  ): Iterable[(Offset, Update)] => Unit = input => {

    if (input.nonEmpty) {

      (for {
        (completionInfo, transactionAccepted) <- input.iterator
          .collect { case (_, ta: TransactionAccepted) => ta }
          .flatMap(ta => ta.completionInfoO.iterator.map(_ -> ta))
        userId = completionInfo.userId
        statistics = TransactionNodeStatistics(
          transactionAccepted.transaction,
          Set.empty[Ref.PackageId],
        )
      } yield (userId, statistics.committed.actions + statistics.rolledBack.actions)).toList
        .groupMapReduce(_._1)(_._2)(_ + _)
        .toList
        .filter(_._2 != 0)
        .sortBy(_._1)
        .foreach { case (userId, count) =>
          withExtraMetricLabels(IndexerMetrics.Labels.userId -> userId) { implicit mc =>
            meteredEventsMeter.mark(count.toLong)
          }
        }
    }
  }
}
