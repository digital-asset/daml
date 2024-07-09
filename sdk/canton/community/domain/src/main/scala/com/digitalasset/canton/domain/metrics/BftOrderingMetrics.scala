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
import com.digitalasset.canton.logging.pretty.PrettyBareCase
import com.digitalasset.canton.metrics.{DbStorageHistograms, DbStorageMetrics}

class BftOrderingHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix = parent :+ BftOrderingMetrics.Prefix
  private[metrics] val dbStorage = new DbStorageHistograms(parent)

  private[metrics] val requestsOrderingTime: Item = Item(
    prefix :+ "requests-ordering-time",
    summary = "Requests ordering time",
    description =
      """Records the rate and time it takes to order requests. This metric is always meaningful
        |when queried on and restricted to the receiving sequencer; in other cases, it is meaningful only
        |when the receiving and reporting sequencers' clocks are kept synchronized.""",
    qualification = MetricQualification.Latency,
  )

  private[metrics] val requestsSize: Item = Item(
    prefix :+ "requests-size",
    summary = "Requests ordering time",
    description = """Records the size of requests to the BFT ordering service""",
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

  object dbStorage extends DbStorageMetrics(histograms.dbStorage, openTelemetryMetricsFactory)

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

    val requestsOrdered: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ s"ordered-requests",
        summary = "Requests ordered",
        description = "Measures the total requests ordered.",
        qualification = MetricQualification.Traffic,
      )
    )

    val batchesOrdered: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ s"ordered-batches",
        summary = "Batches ordered",
        description = "Measures the total batches ordered.",
        qualification = MetricQualification.Traffic,
      )
    )

    val blocksOrdered: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ s"ordered-blocks",
        summary = "Blocks ordered",
        description = "Measures the total blocks ordered.",
        qualification = MetricQualification.Traffic,
      )
    )

    object requestsOrderingTime {
      val timer: Timer =
        openTelemetryMetricsFactory.timer(histograms.requestsOrderingTime.info)

      object labels {
        val ReceivingSequencer: String = "receivingSequencer"
      }
    }
  }

  object ingress {
    private val prefix = BftOrderingMetrics.this.prefix :+ "ingress"

    val requestsReceived: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ s"received-requests",
        summary = "Requests received",
        description = "Measures the total requests received.",
        qualification = MetricQualification.Traffic,
      )
    )

    val bytesReceived: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ s"received-bytes",
        summary = "Bytes received",
        description = "Measures the total bytes received.",
        qualification = MetricQualification.Traffic,
      )
    )

    val requestsSize: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.requestsSize.info)

    object labels {
      val Tag: String = "tag"
      val Sender: String = "sender"
      val ForSequencer: String = "forSequencer"

      object outcome {
        val Key: String = "outcome"

        object values {
          sealed trait OutcomeValue extends Product with PrettyBareCase
          case object Success extends OutcomeValue
          case object QueueFull extends OutcomeValue
          case object RequestTooBig extends OutcomeValue
        }
      }
    }
  }
}

object BftOrderingMetrics {
  val Prefix: MetricName = MetricName("bftordering")
}
