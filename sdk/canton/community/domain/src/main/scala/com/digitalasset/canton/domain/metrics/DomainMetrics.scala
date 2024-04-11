// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics


import com.daml.metrics.api.MetricHandle.{Counter, Gauge, LabeledMetricsFactory, Meter}
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.MetricQualification.{Debug, Traffic}
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}
import com.daml.metrics.grpc.{DamlGrpcServerMetrics, GrpcServerMetrics}
import com.daml.metrics.{CacheMetrics, HealthMetrics}
import com.digitalasset.canton.environment.BaseMetrics
import com.digitalasset.canton.metrics.{DbStorageMetrics, SequencerClientMetrics}
import com.google.common.annotations.VisibleForTesting

class SequencerMetrics(
    parent: MetricName,
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
    val grpcMetrics: GrpcServerMetrics,
    val healthMetrics: HealthMetrics,
) extends BaseMetrics {
  override val prefix: MetricName = parent
  private implicit val mc: MetricsContext = MetricsContext.Empty
  override def storageMetrics: DbStorageMetrics = dbStorage

  object sequencerClient extends SequencerClientMetrics(prefix, openTelemetryMetricsFactory)

  @MetricDoc.Tag(
    summary = "Number of active sequencer subscriptions",
    description =
      """This metric indicates the number of active subscriptions currently open and actively
        |served subscriptions at the sequencer.""",
    qualification = Debug,
  )
  val subscriptionsGauge: Gauge[Int] =
    openTelemetryMetricsFactory.gauge[Int](MetricName(prefix :+ "subscriptions"), 0)(
      MetricsContext.Empty
    )
  @MetricDoc.Tag(
    summary = "Number of messages processed by the sequencer",
    description = """This metric measures the number of successfully validated messages processed
                    |by the sequencer since the start of this process.""",
    qualification = Debug,
  )
  val messagesProcessed: Meter = openTelemetryMetricsFactory.meter(prefix :+ "processed")

  @MetricDoc.Tag(
    summary = "Number of message bytes processed by the sequencer",
    description =
      """This metric measures the total number of message bytes processed by the sequencer.
        |If the message received by the sequencer contains duplicate or irrelevant fields,
        |the contents of these fields do not contribute to this metric.""",
    qualification = Debug,
  )
  val bytesProcessed: Meter = openTelemetryMetricsFactory.meter(prefix :+ "processed-bytes")

  @MetricDoc.Tag(
    summary = "Number of time requests received by the sequencer",
    description =
      """When a Participant needs to know the domain time it will make a request for a time proof to be sequenced.
        |It would be normal to see a small number of these being sequenced, however if this number becomes a significant
        |portion of the total requests to the sequencer it could indicate that the strategy for requesting times may
        |need to be revised to deal with different clock skews and latencies between the sequencer and participants.""",
    qualification = Debug,
  )
  val timeRequests: Meter = openTelemetryMetricsFactory.meter(prefix :+ "time-requests")

  @MetricDoc.Tag(
    summary = "Age of oldest unpruned sequencer event.",
    description =
      """This gauge exposes the age of the oldest, unpruned sequencer event in hours as a way to quantify the
        |pruning backlog.""",
    qualification = Debug,
  )
  val maxEventAge: Gauge[Long] =
    openTelemetryMetricsFactory.gauge[Long](MetricName(prefix :+ "max-event-age"), 0L)(
      MetricsContext.Empty
    )

  object dbStorage extends DbStorageMetrics(prefix, openTelemetryMetricsFactory)

  // TODO(i14580): add testing
  object trafficControl {
    private val prefix: MetricName = SequencerMetrics.this.prefix :+ "traffic-control"

    val balanceCache: CacheMetrics =
      new CacheMetrics(prefix :+ "balance-cache", openTelemetryMetricsFactory)

    @MetricDoc.Tag(
      summary = "Raw size of an event received in the sequencer.",
      description =
        """This the raw payload size of an event, on the write path. Final event cost calculation.""",
      qualification = Traffic,
    )
    val eventReceived: Meter = openTelemetryMetricsFactory.meter(prefix :+ "event-received-size")

    @MetricDoc.Tag(
      summary = "Cost of rejected event.",
      description =
        """Cost of an event that was rejected because it exceeded the sender's traffic limit.""",
      qualification = Traffic,
    )
    val eventRejected: Meter = openTelemetryMetricsFactory.meter(prefix :+ "event-rejected-cost")

    @MetricDoc.Tag(
      summary = "Cost of delivered event.",
      description = """Cost of an event that was delivered.""",
      qualification = Traffic,
    )
    val eventDelivered: Meter = openTelemetryMetricsFactory.meter(prefix :+ "event-delivered-cost")

    @MetricDoc.Tag(
      summary = "Counts balance updates fully processed by the sequencer.",
      description = """Value of balance updates for all (aggregated).""",
      qualification = Traffic,
    )
    val balanceUpdateProcessed: Counter =
      openTelemetryMetricsFactory.counter(prefix :+ "balance-update")

    @MetricDoc.Tag(
      summary = "Counts cache misses when trying to retrieve a balance for a given timestamp.",
      description = """The per member cache only keeps in memory a subset of all the non-pruned balance updates persisted in the database.
          |If the cache contains *some* balances for a member but not the one requested, a DB call will be made to try to retrieve it.
          |When that happens, this metric is incremented. If this occurs too frequently, consider increasing the config value of trafficBalanceCacheSizePerMember.""",
      qualification = Traffic,
    )
    val balanceCacheMissesForTimestamp: Counter =
      openTelemetryMetricsFactory.counter(prefix :+ "balance-cache-miss-for-timestamp")
  }
}

object SequencerMetrics {

  @VisibleForTesting
  def noop(testName: String) = new SequencerMetrics(
    MetricName(testName),
    NoOpMetricsFactory,
    new DamlGrpcServerMetrics(NoOpMetricsFactory, "sequencer"),
    new HealthMetrics(NoOpMetricsFactory),
  )

}

class MediatorMetrics(
    parent: MetricName,
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
    val grpcMetrics: GrpcServerMetrics,
    val healthMetrics: HealthMetrics,
) extends BaseMetrics {

  val prefix: MetricName = parent
  private implicit val mc: MetricsContext = MetricsContext.Empty

  override def storageMetrics: DbStorageMetrics = dbStorage

  object dbStorage extends DbStorageMetrics(prefix, openTelemetryMetricsFactory)

  object sequencerClient extends SequencerClientMetrics(prefix, openTelemetryMetricsFactory)

  @MetricDoc.Tag(
    summary = "Number of currently outstanding requests",
    description = """This metric provides the number of currently open requests registered
                    |with the mediator.""",
    qualification = Debug,
  )
  val outstanding: Gauge[Int] =
    openTelemetryMetricsFactory.gauge(prefix :+ "outstanding-requests", 0)(MetricsContext.Empty)

  @MetricDoc.Tag(
    summary = "Number of totally processed requests",
    description = """This metric provides the number of totally processed requests since the system
                    |has been started.""",
    qualification = Debug,
  )
  val requests: Meter = openTelemetryMetricsFactory.meter(prefix :+ "requests")

  @MetricDoc.Tag(
    summary = "Age of oldest unpruned confirmation response.",
    description =
      """This gauge exposes the age of the oldest, unpruned confirmation response in hours as a way to quantify the
        |pruning backlog.""",
    qualification = Debug,
  )
  val maxEventAge: Gauge[Long] =
    openTelemetryMetricsFactory.gauge[Long](MetricName(prefix :+ "max-event-age"), 0L)(
      MetricsContext.Empty
    )

  // TODO(i14580): add testing
  object trafficControl {
    @MetricDoc.Tag(
      summary = "Event rejected because of traffic limit exceeded",
      description =
        """This metric is being incremented every time a sequencer rejects an event because
           the sender does not have enough credit.""",
      qualification = Traffic,
    )
    val eventRejected: Meter = openTelemetryMetricsFactory.meter(prefix :+ "event-rejected")
  }
}
