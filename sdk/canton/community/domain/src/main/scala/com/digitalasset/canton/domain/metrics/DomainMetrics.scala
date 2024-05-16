// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.daml.metrics.grpc.{DamlGrpcServerMetrics, GrpcServerMetrics}
import com.daml.metrics.{CacheMetrics, HealthMetrics}
import com.digitalasset.canton.environment.BaseMetrics
import com.digitalasset.canton.metrics.{
  DbStorageHistograms,
  DbStorageMetrics,
  HistogramInventory,
  SequencerClientHistograms,
  SequencerClientMetrics,
}
import com.google.common.annotations.VisibleForTesting

class SequencerHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix = parent :+ "sequencer"
  private[metrics] val sequencerClient = new SequencerClientHistograms(parent)
  private[metrics] val dbStorage = new DbStorageHistograms(parent)
}

class SequencerMetrics(
    histograms: SequencerHistograms,
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
    val grpcMetrics: GrpcServerMetrics,
    val healthMetrics: HealthMetrics,
) extends BaseMetrics {
  override val prefix: MetricName = histograms.prefix
  private implicit val mc: MetricsContext = MetricsContext.Empty
  override def storageMetrics: DbStorageMetrics = dbStorage

  object block extends BlockMetrics(prefix, openTelemetryMetricsFactory)

  object sequencerClient
      extends SequencerClientMetrics(histograms.sequencerClient, openTelemetryMetricsFactory)

  object publicApi {
    private val prefix = SequencerMetrics.this.prefix :+ "public-api"
    val subscriptionsGauge: Gauge[Int] =
      openTelemetryMetricsFactory.gauge[Int](
        MetricInfo(
          prefix :+ "subscriptions",
          summary = "Number of active sequencer subscriptions",
          description =
            """This metric indicates the number of active subscriptions currently open and actively
            |served subscriptions at the sequencer.""",
          qualification = MetricQualification.Traffic,
        ),
        0,
      )

    val messagesProcessed: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "processed",
        summary = "Number of messages processed by the sequencer",
        description =
          """This metric measures the number of successfully validated messages processed
          |by the sequencer since the start of this process.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val bytesProcessed: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ s"processed-${Histogram.Bytes}",
        summary = "Number of message bytes processed by the sequencer",
        description =
          """This metric measures the total number of message bytes processed by the sequencer.
          |If the message received by the sequencer contains duplicate or irrelevant fields,
          |the contents of these fields do not contribute to this metric.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val timeRequests: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "time-requests",
        summary = "Number of time requests received by the sequencer",
        description =
          """When a Participant needs to know the domain time it will make a request for a time proof to be sequenced.
          |It would be normal to see a small number of these being sequenced, however if this number becomes a significant
          |portion of the total requests to the sequencer it could indicate that the strategy for requesting times may
          |need to be revised to deal with different clock skews and latencies between the sequencer and participants.""",
        qualification = MetricQualification.Debug,
      )
    )
  }

  val maxEventAge: Gauge[Long] =
    openTelemetryMetricsFactory.gauge[Long](
      MetricInfo(
        prefix :+ "max-event-age",
        summary = "Age of oldest unpruned sequencer event.",
        description =
          """This gauge exposes the age of the oldest, unpruned sequencer event in hours as a way to quantify the
            |pruning backlog.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  object dbStorage extends DbStorageMetrics(histograms.dbStorage, openTelemetryMetricsFactory)

  // TODO(i14580): add testing
  object trafficControl {
    private val prefix: MetricName = SequencerMetrics.this.prefix :+ "traffic-control"

    val balanceCache: CacheMetrics =
      new CacheMetrics(prefix :+ "balance-cache", openTelemetryMetricsFactory)

    val eventReceived: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "event-received-size",
        summary = "Raw size of an event received in the sequencer.",
        description =
          """This the raw payload size of an event, on the write path. Final event cost calculation.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val eventRejected: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "event-rejected-cost",
        summary = "Cost of rejected event.",
        description =
          """Cost of an event that was rejected because it exceeded the sender's traffic limit.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val eventDelivered: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "event-delivered-cost",
        summary = "Cost of delivered event.",
        description = """Cost of an event that was delivered.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val balanceUpdateProcessed: Counter =
      openTelemetryMetricsFactory.counter(
        MetricInfo(
          prefix :+ "balance-update",
          summary = "Counts balance updates fully processed by the sequencer.",
          description = """Value of balance updates for all (aggregated).""",
          qualification = MetricQualification.Traffic,
        )
      )

    val balanceCacheMissesForTimestamp: Counter =
      openTelemetryMetricsFactory.counter(
        MetricInfo(
          prefix :+ "balance-cache-miss-for-timestamp",
          summary = "Counts cache misses when trying to retrieve a balance for a given timestamp.",
          description = """The per member cache only keeps in memory a subset of all the non-pruned balance updates persisted in the database.
                        |If the cache contains *some* balances for a member but not the one requested, a DB call will be made to try to retrieve it.
                        |When that happens, this metric is incremented. If this occurs too frequently, consider increasing the config value of trafficPurchasedCacheSizePerMember.""",
          qualification = MetricQualification.Debug,
        )
      )
  }
}

object SequencerMetrics {

  @VisibleForTesting
  def noop(testName: String) = new SequencerMetrics(
    new SequencerHistograms(MetricName(testName))(new HistogramInventory),
    NoOpMetricsFactory,
    new DamlGrpcServerMetrics(NoOpMetricsFactory, "sequencer"),
    new HealthMetrics(NoOpMetricsFactory),
  )

}

class MediatorHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix = parent :+ "mediator"
  private[metrics] val sequencerClient = new SequencerClientHistograms(parent)
  private[metrics] val dbStorage = new DbStorageHistograms(parent)

}
class MediatorMetrics(
    histograms: MediatorHistograms,
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
    val grpcMetrics: GrpcServerMetrics,
    val healthMetrics: HealthMetrics,
) extends BaseMetrics {

  override val prefix: MetricName = histograms.prefix
  private implicit val mc: MetricsContext = MetricsContext.Empty

  override def storageMetrics: DbStorageMetrics = dbStorage

  object dbStorage extends DbStorageMetrics(histograms.dbStorage, openTelemetryMetricsFactory)

  object sequencerClient
      extends SequencerClientMetrics(histograms.sequencerClient, openTelemetryMetricsFactory)

  val outstanding: Gauge[Int] =
    openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "outstanding-requests",
        summary = "Number of currently outstanding requests",
        description = """This metric provides the number of currently open requests registered
                      |with the mediator.""",
        qualification = MetricQualification.Debug,
      ),
      0,
    )(MetricsContext.Empty)

  val requests: Meter = openTelemetryMetricsFactory.meter(
    MetricInfo(
      prefix :+ "requests",
      summary = "Number of totally processed requests",
      description =
        """This metric provides the number of totally processed requests since the system
                    |has been started.""",
      qualification = MetricQualification.Debug,
    )
  )

  val maxEventAge: Gauge[Long] =
    openTelemetryMetricsFactory.gauge[Long](
      MetricInfo(
        prefix :+ "max-event-age",
        summary = "Age of oldest unpruned confirmation response.",
        description =
          """This gauge exposes the age of the oldest, unpruned confirmation response in hours as a way to quantify the
          |pruning backlog.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )(
      MetricsContext.Empty
    )

  // TODO(i14580): add testing
  object trafficControl {
    val eventRejected: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "event-rejected",
        summary = "Event rejected because of traffic limit exceeded",
        description =
          """This metric is being incremented every time a sequencer rejects an event because
           the sender does not have enough credit.""",
        qualification = MetricQualification.Traffic,
      )
    )
  }
}
