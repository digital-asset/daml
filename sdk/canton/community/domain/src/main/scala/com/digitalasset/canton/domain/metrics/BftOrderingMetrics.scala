// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Histogram, LabeledMetricsFactory, Meter, Timer}
import com.daml.metrics.api.{
  HistogramInventory,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}
import com.daml.metrics.grpc.GrpcServerMetrics
import com.digitalasset.canton.environment.BaseMetrics
import com.digitalasset.canton.metrics.{DbStorageHistograms, DbStorageMetrics}

class BftOrderingHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix = parent :+ BftOrderingMetrics.Prefix
  private[metrics] val dbStorage = new DbStorageHistograms(parent)

  private[metrics] val outputStorageTime: Item = Item(
    prefix :+ "output-storage-time",
    summary = "Block store time and rate",
    description = "Records the rate and time it takes to perform block store operations.",
    qualification = MetricQualification.Debug,
  )
}

class BftOrderingMetrics(
    histograms: BftOrderingHistograms,
    override val openTelemetryMetricsFactory: LabeledMetricsFactory,
    override val grpcMetrics: GrpcServerMetrics,
    override val healthMetrics: HealthMetrics,
) extends BaseMetrics {

  override val prefix: MetricName = histograms.prefix

  private implicit val mc: MetricsContext = MetricsContext.Empty

  override def storageMetrics: DbStorageMetrics = dbStorage

  object dbStorage extends DbStorageMetrics(histograms.dbStorage, openTelemetryMetricsFactory) {

    sealed trait Op extends Product
    object Op {
      val Key: String = classOf[Op].getSimpleName

      case object Store extends Op
    }

    object output {
      val storageTime: Timer = openTelemetryMetricsFactory.timer(histograms.outputStorageTime.info)
    }
  }

  object global {

    private val prefix = BftOrderingMetrics.this.prefix :+ "global"

    val bytesOrdered: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ s"ordered-${Histogram.Bytes}",
        summary = "Bytes ordered",
        description = "Measures the total bytes ordered.",
        qualification = MetricQualification.Traffic,
      )
    )
  }
}

object BftOrderingMetrics {
  val Prefix: MetricName = MetricName("bftordering")
}
